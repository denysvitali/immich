import 'dart:async';
import 'package:http/http.dart';
import 'package:immich_mobile/services/http_client_config.service.dart';
import 'package:logging/logging.dart';

class _ImmichHttpClientSingleton {
  static _ImmichHttpClientSingleton? _instance;
  Client? _client;
  final HttpClientConfigService _configService = HttpClientConfigService();
  final Logger _log = Logger('ImmichHttpClient');
  bool _isInitializing = false;

  _ImmichHttpClientSingleton._();

  static _ImmichHttpClientSingleton get instance {
    _instance ??= _ImmichHttpClientSingleton._();
    return _instance!;
  }

  Client getClient() {
    if (_client == null) {
      _log.severe('HTTP client is not initialized! This will cause image loading failures. '
          'Make sure refreshClient() is called during app initialization.');
      throw StateError("HTTP client is not initialized! Call refreshClient() first.");
    }
    return _client!;
  }

  /// Refreshes the HTTP client with proper async handling to avoid main thread deadlocks
  /// Now uses the centralized configuration service for consistent mTLS setup
  Future<void> refreshClient() async {
    if (_isInitializing) {
      _log.warning('HTTP client is already being initialized, skipping duplicate call');
      return;
    }

    _isInitializing = true;
    try {
      _log.info('Starting HTTP client initialization...');

      // Only refresh if client is null or explicitly needed
      if (_client == null) {
        _client = await _configService.refreshClient();
        _log.info('HTTP client initialization completed successfully');
      } else {
        _log.info('HTTP client already initialized, skipping refresh');
      }
    } catch (e, stack) {
      _log.severe('Failed to initialize HTTP client', e, stack);
      rethrow;
    } finally {
      _isInitializing = false;
    }
  }

  void dispose() {
    _client?.close();
    _client = null;
  }
}

/// Creates an optimized HTTP client based on the platform (singleton pattern)
///
/// On Android, uses OkHttpClient with mTLS support for better performance
/// On other platforms, falls back to standard HTTP client
/// Returns the same client instance for all calls after first initialization
/// Now uses centralized configuration service for consistent mTLS setup across all threads
Client immichHttpClient() {
  return _ImmichHttpClientSingleton.instance.getClient();
}

Future<void> refreshClient() async {
  return _ImmichHttpClientSingleton.instance.refreshClient();
}

/// Gets a properly configured HTTP client with mTLS support
/// This is the recommended way to get HTTP clients in isolates and background workers
/// as it ensures proper configuration across all threads
Future<Client> getConfiguredHttpClient() async {
  final configService = HttpClientConfigService();
  return await configService.getConfiguredClient();
}
