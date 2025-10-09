part of 'image_request.dart';

class RemoteImageRequest extends ImageRequest {
  static final log = Logger('RemoteImageRequest');
  final RemoteCacheManager? cacheManager;
  final String uri;
  final Map<String, String> headers;

  RemoteImageRequest({required this.uri, required this.headers, this.cacheManager});

  @override
  Future<ImageInfo?> load(ImageDecoderCallback decode, {double scale = 1.0}) async {
    if (_isCancelled) {
      return null;
    }

    // TODO: the cache manager makes everything sequential with its DB calls and its operations cannot be cancelled,
    //  so it ends up being a bottleneck.  We only prefer fetching from it when it can skip the DB call.
    final cachedFileImage = await _loadCachedFile(uri, decode, scale, inMemoryOnly: true);
    if (cachedFileImage != null) {
      return cachedFileImage;
    }

    try {
      final buffer = await _downloadImage(uri);
      if (buffer == null) {
        return null;
      }

      return await _decodeBuffer(buffer, decode, scale);
    } catch (e) {
      if (_isCancelled) {
        return null;
      }

      final cachedFileImage = await _loadCachedFile(uri, decode, scale, inMemoryOnly: false);
      if (cachedFileImage != null) {
        return cachedFileImage;
      }

      rethrow;
    }
  }

  Future<ImmutableBuffer?> _downloadImage(String url) async {
    if (_isCancelled) {
      return null;
    }

    try {
      // Use immichHttpClient() which has the proper SSL/mTLS configuration
      final httpClient = immichHttpClient();
      final uri = Uri.parse(url);

      // Create headers map for the http client
      final requestHeaders = <String, String>{};
      for (final entry in headers.entries) {
        requestHeaders[entry.key] = entry.value;
      }

      log.fine('Downloading image from: $url');
      final response = await httpClient.get(uri, headers: requestHeaders);
      if (_isCancelled) {
        return null;
      }

      if (response.statusCode != 200) {
        log.warning('Failed to load image from $url: HTTP ${response.statusCode}');
        throw Exception('Failed to load image: ${response.statusCode}');
      }

      log.fine('Successfully downloaded image from: $url (${response.bodyBytes.length} bytes)');

      final cacheManager = this.cacheManager;
      final streamController = StreamController<List<int>>(sync: true);

      // Convert response body to bytes
      final bytes = response.bodyBytes;

      // Set up caching
      cacheManager?.putStreamedFile(url, streamController.stream);

      // Add bytes to stream controller for caching
      if (cacheManager != null) {
        streamController.add(bytes);
      }
      streamController.close();

      return await ImmutableBuffer.fromUint8List(bytes);
    } on StateError catch (e, stack) {
      log.severe('HTTP client not initialized when trying to load image from $url', e, stack);
      rethrow;
    } catch (e, stack) {
      if (_isCancelled) {
        return null;
      }
      log.severe('Error downloading image from $url', e, stack);
      rethrow;
    }
  }


  Future<ImageInfo?> _loadCachedFile(
    String url,
    ImageDecoderCallback decode,
    double scale, {
    required bool inMemoryOnly,
  }) async {
    final cacheManager = this.cacheManager;
    if (_isCancelled || cacheManager == null) {
      return null;
    }

    final file = await (inMemoryOnly ? cacheManager.getFileFromMemory(url) : cacheManager.getFileFromCache(url));
    if (_isCancelled || file == null) {
      return null;
    }

    try {
      final buffer = await ImmutableBuffer.fromFilePath(file.file.path);
      return await _decodeBuffer(buffer, decode, scale);
    } catch (e) {
      log.severe('Failed to decode cached image', e);
      _evictFile(url);
      return null;
    }
  }

  Future<void> _evictFile(String url) async {
    try {
      await cacheManager?.removeFile(url);
    } catch (e) {
      log.severe('Failed to remove cached image', e);
    }
  }

  Future<ImageInfo?> _decodeBuffer(ImmutableBuffer buffer, ImageDecoderCallback decode, scale) async {
    if (_isCancelled) {
      buffer.dispose();
      return null;
    }
    final codec = await decode(buffer);
    if (_isCancelled) {
      buffer.dispose();
      codec.dispose();
      return null;
    }
    final frame = await codec.getNextFrame();
    return ImageInfo(image: frame.image, scale: scale);
  }

  @override
  void _onCancelled() {
    // No need to abort request since we're using the http package's Client
    // which doesn't support cancellation in the same way
  }
}
