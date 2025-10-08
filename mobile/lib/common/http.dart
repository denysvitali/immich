import 'dart:async';
import 'package:http/http.dart';
import 'package:immich_mobile/services/http_client_config.service.dart';

class _ImmichHttpClientSingleton {
  static _ImmichHttpClientSingleton? _instance;
  Client? _client;
  final HttpClientConfigService _configService = HttpClientConfigService();

  _ImmichHttpClientSingleton._();

  static _ImmichHttpClientSingleton get instance {
    _instance ??= _ImmichHttpClientSingleton._();
    return _instance!;
  }

  Client getClient() {
    if (_client == null) {
      throw "Client is not initialized!";
    }
    return _client!;
  }

  /// Refreshes the HTTP client with proper async handling to avoid main thread deadlocks
  /// Now uses the centralized configuration service for consistent mTLS setup
  Future<void> refreshClient() async {
    _client = await _configService.refreshClient();
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
