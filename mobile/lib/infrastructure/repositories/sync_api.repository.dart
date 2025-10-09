import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:immich_mobile/common/http.dart';
import 'package:immich_mobile/constants/constants.dart';
import 'package:immich_mobile/domain/models/store.model.dart';
import 'package:immich_mobile/domain/models/sync_event.model.dart';
import 'package:immich_mobile/entities/store.entity.dart';
import 'package:immich_mobile/services/api.service.dart';
import 'package:logging/logging.dart';
import 'package:openapi/api.dart';

class SyncApiRepository {
  final Logger _logger = Logger('SyncApiRepository');
  final ApiService _api;
  final http.Client httpClient;

  // Use OkHttp client with mTLS - the server returns empty sync streams when there's no data
  SyncApiRepository(this._api, {http.Client? httpClient}) : httpClient = httpClient ?? immichHttpClient();

  Future<void> ack(List<String> data) {
    return _api.syncApi.sendSyncAck(SyncAckSetDto(acks: data));
  }

  Future<void> streamChanges(
    Future<void> Function(List<SyncEvent>, Function() abort, Function() reset) onData, {
    Function()? onReset,
    int batchSize = kSyncEventBatchSize,
  }) async {
    final stopwatch = Stopwatch()..start();
    final endpoint = "${_api.apiClient.basePath}/sync/stream";

    final headers = {'Content-Type': 'application/json', 'Accept': 'application/jsonlines+json'};

    final headerParams = <String, String>{};
    await _api.applyToParams([], headerParams);
    headers.addAll(headerParams);

    var shouldReset = Store.get(StoreKey.shouldResetSync, false);

    // TEMPORARY FIX: Force reset once to clear stale checkpoints after mTLS changes
    // This can be removed after all clients have synced once
    final hasResetAfterMTLS = Store.get(StoreKey.hasResetAfterMTLS, false);
    if (!hasResetAfterMTLS) {
      _logger.warning("Forcing sync reset to clear stale checkpoints after mTLS changes");
      shouldReset = true;
      await Store.put(StoreKey.hasResetAfterMTLS, true);
    }

    final request = http.Request('POST', Uri.parse(endpoint));
    request.headers.addAll(headers);

    final syncRequest = SyncStreamDto(
      types: [
        SyncRequestType.authUsersV1,
        SyncRequestType.usersV1,
        SyncRequestType.assetsV1,
        SyncRequestType.assetExifsV1,
        SyncRequestType.partnersV1,
        SyncRequestType.partnerAssetsV1,
        SyncRequestType.partnerAssetExifsV1,
        SyncRequestType.albumsV1,
        SyncRequestType.albumUsersV1,
        SyncRequestType.albumAssetsV1,
        SyncRequestType.albumAssetExifsV1,
        SyncRequestType.albumToAssetsV1,
        SyncRequestType.memoriesV1,
        SyncRequestType.memoryToAssetsV1,
        SyncRequestType.stacksV1,
        SyncRequestType.partnerStacksV1,
        SyncRequestType.userMetadataV1,
        SyncRequestType.peopleV1,
        SyncRequestType.assetFacesV1,
      ],
      reset: shouldReset,
    );

    request.body = jsonEncode(syncRequest.toJson());

    _logger.info("Sync request details:");
    _logger.info("  Endpoint: $endpoint");
    _logger.info("  Headers: $headers");
    _logger.info("  Reset: $shouldReset");
    _logger.info("  Request types: ${syncRequest.types.length} types");
    _logger.info("  Request body size: ${request.body.length} bytes");

    String previousChunk = '';
    List<String> lines = [];

    bool shouldAbort = false;

    void abort() {
      _logger.warning("Abort requested, stopping sync stream");
      shouldAbort = true;
    }

    final reset = onReset ?? () {};

    try {
      _logger.info("Sending sync stream request to: $endpoint");
      final response = await httpClient
          .send(request)
          .timeout(
            const Duration(minutes: 5), // 5 minute timeout for the initial request
            onTimeout: () {
              throw TimeoutException('Sync stream request timed out after 5 minutes', const Duration(minutes: 5));
            },
          );
      _logger.info("Sync stream request completed with status: ${response.statusCode}");
      _logger.info("Response headers: ${response.headers}");
      _logger.info("Content-Type: ${response.headers['content-type']}");
      _logger.info("Content-Length: ${response.headers['content-length']}");

      if (response.statusCode != 200) {
        final errorBody = await response.stream.bytesToString();
        throw ApiException(response.statusCode, 'Failed to get sync stream: $errorBody');
      }

      // Reset after successful stream start
      await Store.put(StoreKey.shouldResetSync, false);
      _logger.info("Starting to process sync stream data...");

      // Add timeout to prevent hanging indefinitely
      int chunkCount = 0;
      bool streamEnded = false;
      await for (final chunk
          in response.stream
              .transform(utf8.decoder)
              .timeout(
                const Duration(minutes: 2), // 2 minute timeout for stream processing
                onTimeout: (sink) {
                  _logger.warning("Sync stream processing timed out after 2 minutes, received $chunkCount chunks");
                  sink.close();
                },
              )) {
        chunkCount++;
        _logger.info("Received chunk #$chunkCount with ${chunk.length} bytes: ${chunk.substring(0, chunk.length > 200 ? 200 : chunk.length)}");

        if (shouldAbort) {
          _logger.info("Sync stream aborted by client");
          break;
        }

        previousChunk += chunk;
        final parts = previousChunk.toString().split('\n');
        previousChunk = parts.removeLast();
        lines.addAll(parts);

        _logger.info("Current lines buffer size: ${lines.length}, batch size: $batchSize");

        if (lines.length < batchSize) {
          continue;
        }

        await onData(_parseLines(lines), abort, reset);
        lines.clear();
      }
      streamEnded = true;
      _logger.info("Stream loop ended. Total chunks received: $chunkCount");

      if (lines.isNotEmpty && !shouldAbort) {
        _logger.info("Processing remaining ${lines.length} lines");
        await onData(_parseLines(lines), abort, reset);
      } else {
        _logger.warning("No remaining lines to process. Stream ended with $chunkCount chunks");
      }

      _logger.info("Sync stream processing completed (streamEnded: $streamEnded, chunks: $chunkCount)");
    } catch (error, stack) {
      _logger.severe("Sync stream error: $error", error, stack);
      return Future.error(error, stack);
    }
    stopwatch.stop();
    _logger.info("Remote Sync completed in ${stopwatch.elapsed.inMilliseconds}ms");
  }

  List<SyncEvent> _parseLines(List<String> lines) {
    final List<SyncEvent> data = [];

    for (final line in lines) {
      final jsonData = jsonDecode(line);
      final type = SyncEntityType.fromJson(jsonData['type'])!;
      final dataJson = jsonData['data'];
      final ack = jsonData['ack'];

      // Log People and Face related data
      if (type == SyncEntityType.personV1 || type == SyncEntityType.assetFaceV1) {
        _logger.info("Received sync data for $type");
      }

      final converter = _kResponseMap[type];
      if (converter == null) {
        _logger.warning("Unknown type $type");
        continue;
      }

      data.add(SyncEvent(type: type, data: converter(dataJson), ack: ack));
    }

    _logger.info("Parsed ${data.length} sync events from ${lines.length} lines");
    return data;
  }
}

const _kResponseMap = <SyncEntityType, Function(Object)>{
  SyncEntityType.authUserV1: SyncAuthUserV1.fromJson,
  SyncEntityType.userV1: SyncUserV1.fromJson,
  SyncEntityType.userDeleteV1: SyncUserDeleteV1.fromJson,
  SyncEntityType.partnerV1: SyncPartnerV1.fromJson,
  SyncEntityType.partnerDeleteV1: SyncPartnerDeleteV1.fromJson,
  SyncEntityType.assetV1: SyncAssetV1.fromJson,
  SyncEntityType.assetDeleteV1: SyncAssetDeleteV1.fromJson,
  SyncEntityType.assetExifV1: SyncAssetExifV1.fromJson,
  SyncEntityType.partnerAssetV1: SyncAssetV1.fromJson,
  SyncEntityType.partnerAssetBackfillV1: SyncAssetV1.fromJson,
  SyncEntityType.partnerAssetDeleteV1: SyncAssetDeleteV1.fromJson,
  SyncEntityType.partnerAssetExifV1: SyncAssetExifV1.fromJson,
  SyncEntityType.partnerAssetExifBackfillV1: SyncAssetExifV1.fromJson,
  SyncEntityType.albumV1: SyncAlbumV1.fromJson,
  SyncEntityType.albumDeleteV1: SyncAlbumDeleteV1.fromJson,
  SyncEntityType.albumUserV1: SyncAlbumUserV1.fromJson,
  SyncEntityType.albumUserBackfillV1: SyncAlbumUserV1.fromJson,
  SyncEntityType.albumUserDeleteV1: SyncAlbumUserDeleteV1.fromJson,
  SyncEntityType.albumAssetCreateV1: SyncAssetV1.fromJson,
  SyncEntityType.albumAssetUpdateV1: SyncAssetV1.fromJson,
  SyncEntityType.albumAssetBackfillV1: SyncAssetV1.fromJson,
  SyncEntityType.albumAssetExifCreateV1: SyncAssetExifV1.fromJson,
  SyncEntityType.albumAssetExifUpdateV1: SyncAssetExifV1.fromJson,
  SyncEntityType.albumAssetExifBackfillV1: SyncAssetExifV1.fromJson,
  SyncEntityType.albumToAssetV1: SyncAlbumToAssetV1.fromJson,
  SyncEntityType.albumToAssetBackfillV1: SyncAlbumToAssetV1.fromJson,
  SyncEntityType.albumToAssetDeleteV1: SyncAlbumToAssetDeleteV1.fromJson,
  SyncEntityType.syncAckV1: _SyncEmptyDto.fromJson,
  SyncEntityType.syncResetV1: _SyncEmptyDto.fromJson,
  SyncEntityType.memoryV1: SyncMemoryV1.fromJson,
  SyncEntityType.memoryDeleteV1: SyncMemoryDeleteV1.fromJson,
  SyncEntityType.memoryToAssetV1: SyncMemoryAssetV1.fromJson,
  SyncEntityType.memoryToAssetDeleteV1: SyncMemoryAssetDeleteV1.fromJson,
  SyncEntityType.stackV1: SyncStackV1.fromJson,
  SyncEntityType.stackDeleteV1: SyncStackDeleteV1.fromJson,
  SyncEntityType.partnerStackV1: SyncStackV1.fromJson,
  SyncEntityType.partnerStackBackfillV1: SyncStackV1.fromJson,
  SyncEntityType.partnerStackDeleteV1: SyncStackDeleteV1.fromJson,
  SyncEntityType.userMetadataV1: SyncUserMetadataV1.fromJson,
  SyncEntityType.userMetadataDeleteV1: SyncUserMetadataDeleteV1.fromJson,
  SyncEntityType.personV1: SyncPersonV1.fromJson,
  SyncEntityType.personDeleteV1: SyncPersonDeleteV1.fromJson,
  SyncEntityType.assetFaceV1: SyncAssetFaceV1.fromJson,
  SyncEntityType.assetFaceDeleteV1: SyncAssetFaceDeleteV1.fromJson,
  SyncEntityType.syncCompleteV1: _SyncEmptyDto.fromJson,
};

class _SyncEmptyDto {
  static _SyncEmptyDto? fromJson(dynamic _) => _SyncEmptyDto();
}
