import 'dart:async';
import 'dart:io';
import 'package:flutter/foundation.dart';
import 'package:http/http.dart';
import 'package:http/io_client.dart';
import 'package:immich_mobile/entities/store.entity.dart';
import 'package:immich_mobile/utils/user_agent.dart';
import 'package:ok_http/ok_http.dart';
import 'package:logging/logging.dart';

/// Centralized service for managing HTTP client configuration across all threads
/// Ensures consistent mTLS configuration and proper client initialization
class HttpClientConfigService {
  static final HttpClientConfigService _instance = HttpClientConfigService._internal();
  factory HttpClientConfigService() => _instance;
  HttpClientConfigService._internal();

  final Logger _log = Logger('HttpClientConfigService');
  Client? _cachedClient;
  String? _lastConfigHash;
  final Map<String, (PrivateKey?, List<X509Certificate>?)> _certCache = {};

  /// Gets a properly configured HTTP client with mTLS support
  /// This method is thread-safe and caches clients based on configuration
  Future<Client> getConfiguredClient() async {
    final configHash = await _getConfigHash();

    if (_cachedClient == null || _lastConfigHash != configHash) {
      _log.info('Creating new HTTP client with config hash: $configHash');
      _cachedClient = await _createConfiguredClient();
      _lastConfigHash = configHash;
    }

    return _cachedClient!;
  }

  /// Forces refresh of the HTTP client (useful when configuration changes)
  Future<Client> refreshClient() async {
    _log.info('Forcing HTTP client refresh');
    _cachedClient = null;
    _lastConfigHash = null;
    return await getConfiguredClient();
  }

  /// Gets a hash of the current configuration to detect changes
  Future<String> _getConfigHash() async {
    final pKeyAlias = SSLClientCertStoreVal.load()?.privateKeyAlias ?? "";
    final userAgent = getUserAgentString();
    final platform = Platform.isAndroid ? 'android' : 'ios';
    return '$pKeyAlias-$userAgent-$platform';
  }

  /// Creates a new HTTP client with proper mTLS configuration
  Future<Client> _createConfiguredClient() async {
    final userAgent = getUserAgentString();

    if (Platform.isAndroid) {
      return await _createAndroidClient(userAgent);
    } else if (Platform.isIOS) {
      return await _createIOSClient(userAgent);
    } else {
      return Future.value(Client());
    }
  }

  /// Creates Android HTTP client with mTLS support using OkHttp
  Future<Client> _createAndroidClient(String userAgent) async {
    final pKeyAlias = SSLClientCertStoreVal.load()?.privateKeyAlias ?? "";
    PrivateKey? pKey;
    List<X509Certificate>? certs;

    if (pKeyAlias.isNotEmpty) {
      try {
        _log.info('Loading mTLS certificates for alias: $pKeyAlias');
        (pKey, certs) = await _loadCertificates(pKeyAlias);
        _log.info('Successfully loaded mTLS certificates');
      } catch (e, stack) {
        _log.warning('Failed to load mTLS certificates: $e', e, stack);
        // Continue without mTLS if certificate loading fails
      }
    }

    final okHttpClient = OkHttpClient(
      configuration: OkHttpClientConfiguration(
        clientPrivateKey: pKey,
        clientCertificateChain: certs,
        validateServerCertificates: true,
        userAgent: userAgent,
        connectTimeout: const Duration(seconds: 30),
        readTimeout: const Duration(minutes: 10), // Increased for long-running sync operations
        writeTimeout: const Duration(seconds: 30),
      ),
    );

    return okHttpClient;
  }

  /// Creates iOS HTTP client (mTLS support to be implemented)
  Future<Client> _createIOSClient(String userAgent) async {
    final httpClient = HttpClient();
    httpClient.userAgent = userAgent;
    httpClient.connectionTimeout = const Duration(seconds: 30);
    httpClient.idleTimeout = const Duration(minutes: 10); // Increased for long-running sync operations

    // TODO: Implement iOS mTLS support
    // For now, just return standard client
    _log.info('Created iOS HTTP client (mTLS not yet implemented)');

    return IOClient(httpClient);
  }

  /// Thread-safe certificate loading with caching
  Future<(PrivateKey?, List<X509Certificate>?)> _loadCertificates(String alias) async {
    if (_certCache.containsKey(alias)) {
      _log.info('Using cached certificates for alias: $alias');
      return _certCache[alias]!;
    }

    _log.info('Loading certificates from compute isolate for alias: $alias');
    final result = await compute(_loadPrivateKeyAndCertificateChainFromAliasCompute, alias);
    _certCache[alias] = result;

    return result;
  }

  /// Clears the certificate cache (useful when certificates change)
  void clearCertificateCache() {
    _log.info('Clearing certificate cache');
    _certCache.clear();
  }

  /// Clears all caches and forces recreation of client
  void clearAllCaches() {
    _log.info('Clearing all HTTP client caches');
    _cachedClient = null;
    _lastConfigHash = null;
    _certCache.clear();
  }
}

/// Top-level function for compute isolate to load private key and certificate chain
/// This must be top-level to work with compute()
(PrivateKey?, List<X509Certificate>?) _loadPrivateKeyAndCertificateChainFromAliasCompute(String alias) {
  PrivateKey? pkey;
  List<X509Certificate>? certs;
  (pkey, certs) = loadPrivateKeyAndCertificateChainFromAlias(alias);
  return (pkey, certs);
}
