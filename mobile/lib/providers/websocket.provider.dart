import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:collection/collection.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:http/http.dart';
import 'package:immich_mobile/common/http.dart';
import 'package:immich_mobile/domain/models/store.model.dart';
import 'package:immich_mobile/services/http_client_config.service.dart';
import 'package:immich_mobile/entities/asset.entity.dart';
import 'package:immich_mobile/entities/store.entity.dart';
import 'package:immich_mobile/models/server_info/server_version.model.dart';
import 'package:immich_mobile/providers/asset.provider.dart';
import 'package:immich_mobile/providers/auth.provider.dart';
import 'package:immich_mobile/providers/background_sync.provider.dart';
import 'package:immich_mobile/providers/db.provider.dart';
import 'package:immich_mobile/providers/server_info.provider.dart';
import 'package:immich_mobile/services/api.service.dart';
import 'package:immich_mobile/services/sync.service.dart';
import 'package:immich_mobile/utils/debounce.dart';
import 'package:logging/logging.dart';
import 'package:ok_http/ok_http.dart';
import 'package:openapi/api.dart';
import 'package:socket_io_client/socket_io_client.dart';
import 'package:immich_mobile/utils/debug_print.dart';
import 'package:web_socket/web_socket.dart' as ws;

enum PendingAction { assetDelete, assetUploaded, assetHidden, assetTrash }

class PendingChange {
  final String id;
  final PendingAction action;
  final dynamic value;

  const PendingChange(this.id, this.action, this.value);

  @override
  String toString() => 'PendingChange(id: $id, action: $action, value: $value)';

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;

    return other is PendingChange && other.id == id && other.action == action;
  }

  @override
  int get hashCode => id.hashCode ^ action.hashCode;
}

class WebsocketState {
  final Socket? socket;
  final bool isConnected;
  final List<PendingChange> pendingChanges;

  const WebsocketState({this.socket, required this.isConnected, required this.pendingChanges});

  WebsocketState copyWith({Socket? socket, bool? isConnected, List<PendingChange>? pendingChanges}) {
    return WebsocketState(
      socket: socket ?? this.socket,
      isConnected: isConnected ?? this.isConnected,
      pendingChanges: pendingChanges ?? this.pendingChanges,
    );
  }

  @override
  String toString() => 'WebsocketState(socket: $socket, isConnected: $isConnected)';

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;

    return other is WebsocketState && other.socket == socket && other.isConnected == isConnected;
  }

  @override
  int get hashCode => socket.hashCode ^ isConnected.hashCode;
}

/// WebSocket adapter that converts OkHttpWebSocket to standard Dart WebSocket
class OkHttpWebSocketAdapter implements WebSocket {
  final ws.WebSocket _okHttpWebSocket;
  final StreamController<dynamic> _controller = StreamController<dynamic>.broadcast();
  bool _isClosed = false;

  OkHttpWebSocketAdapter(this._okHttpWebSocket) {
    // Listen to OkHttpWebSocket events and convert them to standard WebSocket format
    _okHttpWebSocket.events.listen(
      (event) {
        switch (event) {
          case ws.TextDataReceived(text: final text):
            _controller.add(text);
          case ws.BinaryDataReceived(data: final data):
            _controller.add(data);
          case ws.CloseReceived():
            _isClosed = true;
            _controller.close();
        }
      },
      onError: (error) {
        _controller.addError(error);
      },
      onDone: () {
        _isClosed = true;
        _controller.close();
      },
    );
  }

  Stream<dynamic> get stream => _controller.stream;

  @override
  bool get isBroadcast => stream.isBroadcast;

  @override
  void add(dynamic data) {
    if (!_isClosed) {
      if (data is String) {
        _okHttpWebSocket.sendText(data);
      } else if (data is List<int>) {
        _okHttpWebSocket.sendBytes(Uint8List.fromList(data));
      } else {
        _okHttpWebSocket.sendText(data.toString());
      }
    }
  }

  @override
  void addUtf8Text(List<int> bytes) {
    if (!_isClosed) {
      _okHttpWebSocket.sendBytes(Uint8List.fromList(bytes));
    }
  }

  @override
  Future<void> close([int? code, String? reason]) async {
    if (!_isClosed) {
      _isClosed = true;
      await _okHttpWebSocket.close(code, reason);
      _controller.close();
    }
  }

  @override
  int? get closeCode => null; // OkHttpWebSocket doesn't expose this directly

  @override
  String? get closeReason => null; // OkHttpWebSocket doesn't expose this directly

  @override
  String get extensions => '';

  @override
  String get protocol => _okHttpWebSocket.protocol;

  @override
  int get readyState => _isClosed ? 3 : 1; // 1 = OPEN, 3 = CLOSED

  String get url => ''; // OkHttpWebSocket doesn't expose this directly

  // Implement all required WebSocket methods
  @override
  Future<void> addStream(Stream stream) async {
    await for (final data in stream) {
      add(data);
    }
  }

  @override
  Future get done => _controller.done;

  @override
  Duration? get pingInterval => null;

  @override
  set pingInterval(Duration? interval) {
    // OkHttpWebSocket doesn't support ping interval setting
  }

  // Implement all required Stream methods
  @override
  Future<bool> any(bool Function(dynamic element) test) => stream.any(test);

  @override
  Stream<dynamic> asBroadcastStream({
    void Function(StreamSubscription<dynamic> subscription)? onListen,
    void Function(StreamSubscription<dynamic> subscription)? onCancel,
  }) => stream.asBroadcastStream(onListen: onListen, onCancel: onCancel);

  @override
  Stream<S> asyncExpand<S>(Stream<S>? Function(dynamic event) convert) => stream.asyncExpand(convert);

  @override
  Stream<S> asyncMap<S>(FutureOr<S> Function(dynamic event) convert) => stream.asyncMap(convert);

  @override
  Stream<R> cast<R>() => stream.cast<R>();

  @override
  Future<bool> contains(Object? needle) => stream.contains(needle);

  @override
  Stream<dynamic> distinct([bool Function(dynamic previous, dynamic next)? equals]) => stream.distinct(equals);

  @override
  Future<E> drain<E>([E? futureValue]) => stream.drain(futureValue);

  @override
  Future<dynamic> elementAt(int index) => stream.elementAt(index);

  @override
  Future<bool> every(bool Function(dynamic element) test) => stream.every(test);

  @override
  Future<dynamic> firstWhere(bool Function(dynamic element) test, {dynamic Function()? orElse}) =>
      stream.firstWhere(test, orElse: orElse);

  @override
  Future<S> fold<S>(S initialValue, S Function(S previous, dynamic element) combine) =>
      stream.fold(initialValue, combine);

  @override
  Future<void> forEach(void Function(dynamic element) action) => stream.forEach(action);

  @override
  Future<dynamic> get first => stream.first;

  @override
  Future<bool> get isEmpty => stream.isEmpty;

  @override
  Future<dynamic> get last => stream.last;

  @override
  Future<int> get length => stream.length;

  @override
  StreamSubscription<dynamic> listen(
    void Function(dynamic event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) => stream.listen(onData, onError: onError, onDone: onDone, cancelOnError: cancelOnError);

  @override
  Stream<S> map<S>(S Function(dynamic event) convert) => stream.map(convert);

  @override
  Future<dynamic> pipe(StreamConsumer<dynamic> streamConsumer) => stream.pipe(streamConsumer);

  @override
  Future<dynamic> reduce(dynamic Function(dynamic previous, dynamic element) combine) => stream.reduce(combine);

  @override
  Future<dynamic> get single => stream.single;

  @override
  Future<dynamic> singleWhere(bool Function(dynamic element) test, {dynamic Function()? orElse}) =>
      stream.singleWhere(test, orElse: orElse);

  @override
  Stream<dynamic> skip(int count) => stream.skip(count);

  @override
  Stream<dynamic> skipWhile(bool Function(dynamic element) test) => stream.skipWhile(test);

  @override
  Stream<dynamic> take(int count) => stream.take(count);

  @override
  Stream<dynamic> takeWhile(bool Function(dynamic element) test) => stream.takeWhile(test);

  @override
  Stream<dynamic> timeout(Duration timeLimit, {void Function(EventSink<dynamic> sink)? onTimeout}) =>
      stream.timeout(timeLimit, onTimeout: onTimeout);

  @override
  Future<List<dynamic>> toList() => stream.toList();

  @override
  Future<Set<dynamic>> toSet() => stream.toSet();

  @override
  Stream<S> transform<S>(StreamTransformer<dynamic, S> streamTransformer) => stream.transform(streamTransformer);

  @override
  Stream<dynamic> where(bool Function(dynamic event) test) => stream.where(test);

  // Add missing Stream methods
  @override
  Stream<S> expand<S>(Iterable<S> Function(dynamic element) convert) => stream.expand(convert);

  @override
  Stream<dynamic> handleError(Function onError, {bool Function(dynamic error)? test}) =>
      stream.handleError(onError, test: test);

  @override
  Future<String> join([String separator = ""]) => stream.join(separator);

  @override
  Future<dynamic> lastWhere(bool Function(dynamic element) test, {dynamic Function()? orElse}) =>
      stream.lastWhere(test, orElse: orElse);

  // Implement EventSink methods
  @override
  void addError(Object error, [StackTrace? stackTrace]) {
    _controller.addError(error, stackTrace);
  }
}

class ImmichHttpClientAdapter implements HttpClientAdapter {
  Client httpClient = immichHttpClient();
  final _log = Logger('ImmichHttpClientAdapter');
  final _configService = HttpClientConfigService();

  @override
  Future<dynamic> connect(String uri, {Map<String, dynamic>? headers}) async {
    // On Android, try OkHttp first for better mTLS support
    if (Platform.isAndroid) {
      try {
        _log.info('Attempting OkHttpWebSocket connection with mTLS configuration');
        // Get the configured client with mTLS settings
        final httpClient = await _configService.getConfiguredClient();

        // Check if it's actually an OkHttpClient
        if (httpClient is OkHttpClient) {
          _log.info('Using OkHttpClient with mTLS configuration');

          // Create OkHttp WebSocket with the configured client
          final ok = await OkHttpWebSocket.connect(
            Uri.parse(uri),
            client: httpClient,
          );

          _log.info('OkHttpWebSocket successfully connected with mTLS configuration');
          return OkHttpWebSocketAdapter(ok);
        } else {
          _log.warning('Configured client is not OkHttpClient, type: ${httpClient.runtimeType}');
        }
      } catch (e, stack) {
        _log.severe('OkHttpWebSocket connection failed: $e', e, stack);
        // Fall through to try standard WebSocket
      }
    }

    // Fallback to standard WebSocket (or primary method for non-Android)
    final mappedHeaders = headers?.map((k, v) => MapEntry(k, v.toString()));
    try {
      _log.info('Attempting standard WebSocket connection');
      return await WebSocket.connect(uri, headers: mappedHeaders);
    } catch (e, stack) {
      _log.severe('Standard WebSocket.connect failed: $e', e, stack);
      rethrow;
    }
  }
}

class WebsocketNotifier extends StateNotifier<WebsocketState> {
  WebsocketNotifier(this._ref) : super(const WebsocketState(socket: null, isConnected: false, pendingChanges: []));

  final _log = Logger('WebsocketNotifier');
  final Ref _ref;
  final Debouncer _debounce = Debouncer(interval: const Duration(milliseconds: 500));

  final Debouncer _batchDebouncer = Debouncer(
    interval: const Duration(seconds: 5),
    maxWaitTime: const Duration(seconds: 10),
  );
  final List<dynamic> _batchedAssetUploadReady = [];

  @override
  void dispose() {
    _batchDebouncer.dispose();
    super.dispose();
  }

  /// Connects websocket to server unless already connected
  void connect() {
    if (state.isConnected) return;
    final authenticationState = _ref.read(authProvider);

    if (authenticationState.isAuthenticated) {
      try {
        final endpoint = Uri.parse(Store.get(StoreKey.serverEndpoint));
        final headers = ApiService.getRequestHeaders();
        if (endpoint.userInfo.isNotEmpty) {
          headers["Authorization"] = "Basic ${base64.encode(utf8.encode(endpoint.userInfo))}";
        }
        // Provide token via multiple channels: headers, auth, and query
        final wsAuth = {'token': headers['x-immich-user-token'] ?? ''};
        dPrint(() => "Attempting to connect to websocket");
        // Configure socket transports must be specified
        Socket socket = io(
          endpoint.origin,
          OptionBuilder()
              .setPath("${endpoint.path}/socket.io")
              .setTransports(['websocket'])
              .setAuth(wsAuth)
              .setQuery(wsAuth)
              .setHttpClientAdapter(ImmichHttpClientAdapter())
              .enableReconnection()
              .enableForceNew()
              .enableForceNewConnection()
              .enableAutoConnect()
              .setExtraHeaders(headers)
              .build(),
        );

        socket.onConnect((_) {
          dPrint(() => "Established Websocket Connection");
          state = WebsocketState(isConnected: true, socket: socket, pendingChanges: state.pendingChanges);
        });

        socket.onDisconnect((_) {
          dPrint(() => "Disconnect to Websocket Connection");
          state = WebsocketState(isConnected: false, socket: null, pendingChanges: state.pendingChanges);
        });

        socket.on('error', (errorMessage) {
          _log.severe("Websocket Error - $errorMessage");
          state = WebsocketState(isConnected: false, socket: null, pendingChanges: state.pendingChanges);
        });

        if (!Store.isBetaTimelineEnabled) {
          socket.on('on_upload_success', _handleOnUploadSuccess);
          socket.on('on_asset_delete', _handleOnAssetDelete);
          socket.on('on_asset_trash', _handleOnAssetTrash);
          socket.on('on_asset_restore', _handleServerUpdates);
          socket.on('on_asset_update', _handleServerUpdates);
          socket.on('on_asset_stack_update', _handleServerUpdates);
          socket.on('on_asset_hidden', _handleOnAssetHidden);
        } else {
          socket.on('AssetUploadReadyV1', _handleSyncAssetUploadReady);
        }

        socket.on('on_config_update', _handleOnConfigUpdate);
        socket.on('on_new_release', _handleReleaseUpdates);
      } catch (e) {
        dPrint(() => "[WEBSOCKET] Catch Websocket Error - ${e.toString()}");
      }
    }
  }

  void disconnect() {
    dPrint(() => "Attempting to disconnect from websocket");

    _batchedAssetUploadReady.clear();

    var socket = state.socket?.disconnect();

    if (socket?.disconnected == true) {
      state = WebsocketState(isConnected: false, socket: null, pendingChanges: state.pendingChanges);
    }
  }

  void stopListenToEvent(String eventName) {
    state.socket?.off(eventName);
  }

  void stopListenToOldEvents() {
    state.socket?.off('on_upload_success');
    state.socket?.off('on_asset_delete');
    state.socket?.off('on_asset_trash');
    state.socket?.off('on_asset_restore');
    state.socket?.off('on_asset_update');
    state.socket?.off('on_asset_stack_update');
    state.socket?.off('on_asset_hidden');
  }

  void startListeningToOldEvents() {
    state.socket?.on('on_upload_success', _handleOnUploadSuccess);
    state.socket?.on('on_asset_delete', _handleOnAssetDelete);
    state.socket?.on('on_asset_trash', _handleOnAssetTrash);
    state.socket?.on('on_asset_restore', _handleServerUpdates);
    state.socket?.on('on_asset_update', _handleServerUpdates);
    state.socket?.on('on_asset_stack_update', _handleServerUpdates);
    state.socket?.on('on_asset_hidden', _handleOnAssetHidden);
  }

  void stopListeningToBetaEvents() {
    state.socket?.off('AssetUploadReadyV1');
  }

  void startListeningToBetaEvents() {
    state.socket?.on('AssetUploadReadyV1', _handleSyncAssetUploadReady);
  }

  void listenUploadEvent() {
    dPrint(() => "Start listening to event on_upload_success");
    state.socket?.on('on_upload_success', _handleOnUploadSuccess);
  }

  void addPendingChange(PendingAction action, dynamic value) {
    final now = DateTime.now();
    state = state.copyWith(
      pendingChanges: [...state.pendingChanges, PendingChange(now.millisecondsSinceEpoch.toString(), action, value)],
    );
    _debounce.run(handlePendingChanges);
  }

  Future<void> _handlePendingTrashes() async {
    final trashChanges = state.pendingChanges.where((c) => c.action == PendingAction.assetTrash).toList();
    if (trashChanges.isNotEmpty) {
      List<String> remoteIds = trashChanges.expand((a) => (a.value as List).map((e) => e.toString())).toList();

      await _ref.read(syncServiceProvider).handleRemoteAssetRemoval(remoteIds);
      await _ref.read(assetProvider.notifier).getAllAsset();

      state = state.copyWith(pendingChanges: state.pendingChanges.whereNot((c) => trashChanges.contains(c)).toList());
    }
  }

  Future<void> _handlePendingDeletes() async {
    final deleteChanges = state.pendingChanges.where((c) => c.action == PendingAction.assetDelete).toList();
    if (deleteChanges.isNotEmpty) {
      List<String> remoteIds = deleteChanges.map((a) => a.value.toString()).toList();
      await _ref.read(syncServiceProvider).handleRemoteAssetRemoval(remoteIds);
      state = state.copyWith(pendingChanges: state.pendingChanges.whereNot((c) => deleteChanges.contains(c)).toList());
    }
  }

  Future<void> _handlePendingUploaded() async {
    final uploadedChanges = state.pendingChanges.where((c) => c.action == PendingAction.assetUploaded).toList();
    if (uploadedChanges.isNotEmpty) {
      List<AssetResponseDto?> remoteAssets = uploadedChanges.map((a) => AssetResponseDto.fromJson(a.value)).toList();
      for (final dto in remoteAssets) {
        if (dto != null) {
          final newAsset = Asset.remote(dto);
          await _ref.watch(assetProvider.notifier).onNewAssetUploaded(newAsset);
        }
      }
      state = state.copyWith(
        pendingChanges: state.pendingChanges.whereNot((c) => uploadedChanges.contains(c)).toList(),
      );
    }
  }

  Future<void> _handlingPendingHidden() async {
    final hiddenChanges = state.pendingChanges.where((c) => c.action == PendingAction.assetHidden).toList();
    if (hiddenChanges.isNotEmpty) {
      List<String> remoteIds = hiddenChanges.map((a) => a.value.toString()).toList();
      final db = _ref.watch(dbProvider);
      await db.writeTxn(() => db.assets.deleteAllByRemoteId(remoteIds));

      state = state.copyWith(pendingChanges: state.pendingChanges.whereNot((c) => hiddenChanges.contains(c)).toList());
    }
  }

  Future<void> handlePendingChanges() async {
    await _handlePendingUploaded();
    await _handlePendingDeletes();
    await _handlingPendingHidden();
    await _handlePendingTrashes();
  }

  void _handleOnConfigUpdate(dynamic _) {
    _ref.read(serverInfoProvider.notifier).getServerFeatures();
    _ref.read(serverInfoProvider.notifier).getServerConfig();
  }

  // Refresh updated assets
  void _handleServerUpdates(dynamic _) {
    _ref.read(assetProvider.notifier).getAllAsset();
  }

  void _handleOnUploadSuccess(dynamic data) => addPendingChange(PendingAction.assetUploaded, data);

  void _handleOnAssetDelete(dynamic data) => addPendingChange(PendingAction.assetDelete, data);

  void _handleOnAssetTrash(dynamic data) {
    addPendingChange(PendingAction.assetTrash, data);
  }

  void _handleOnAssetHidden(dynamic data) => addPendingChange(PendingAction.assetHidden, data);

  _handleReleaseUpdates(dynamic data) {
    // Json guard
    if (data is! Map) {
      return;
    }

    final json = data.cast<String, dynamic>();
    final serverVersionJson = json.containsKey('serverVersion') ? json['serverVersion'] : null;
    final releaseVersionJson = json.containsKey('releaseVersion') ? json['releaseVersion'] : null;
    if (serverVersionJson == null || releaseVersionJson == null) {
      return;
    }

    final serverVersionDto = ServerVersionResponseDto.fromJson(serverVersionJson);
    final releaseVersionDto = ServerVersionResponseDto.fromJson(releaseVersionJson);
    if (serverVersionDto == null || releaseVersionDto == null) {
      return;
    }

    final serverVersion = ServerVersion.fromDto(serverVersionDto);
    final releaseVersion = ServerVersion.fromDto(releaseVersionDto);
    _ref.read(serverInfoProvider.notifier).handleNewRelease(serverVersion, releaseVersion);
  }

  void _handleSyncAssetUploadReady(dynamic data) {
    _batchedAssetUploadReady.add(data);
    _batchDebouncer.run(_processBatchedAssetUploadReady);
  }

  void _processBatchedAssetUploadReady() {
    if (_batchedAssetUploadReady.isEmpty) {
      return;
    }

    final isSyncAlbumEnabled = Store.get(StoreKey.syncAlbums, false);
    try {
      unawaited(
        _ref.read(backgroundSyncProvider).syncWebsocketBatch(_batchedAssetUploadReady.toList()).then((_) {
          if (isSyncAlbumEnabled) {
            _ref.read(backgroundSyncProvider).syncLinkedAlbum();
          }
        }),
      );
    } catch (error) {
      _log.severe("Error processing batched AssetUploadReadyV1 events: $error");
    }

    _batchedAssetUploadReady.clear();
  }
}

final websocketProvider = StateNotifierProvider<WebsocketNotifier, WebsocketState>((ref) {
  return WebsocketNotifier(ref);
});
