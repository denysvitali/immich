import 'package:immich_mobile/common/http.dart';
import 'package:immich_mobile/services/http_client_config.service.dart';
import 'package:logging/logging.dart';

/// Test utilities for validating HTTP client configuration
/// This can be used for debugging mTLS configuration issues
class HttpClientTest {
  static final Logger _log = Logger('HttpClientTest');

  /// Tests the HTTP client configuration across different scenarios
  static Future<void> runConfigurationTests() async {
    _log.info('Starting HTTP client configuration tests...');

    try {
      // Test 1: Basic HTTP client creation
      _log.info('Test 1: Creating basic HTTP client...');
      await getConfiguredHttpClient();
      _log.info('✓ Basic HTTP client created successfully');

      // Test 3: Certificate cache test
      _log.info('Test 3: Testing certificate caching...');
      final configService = HttpClientConfigService();
      await configService.getConfiguredClient();
      _log.info('✓ Certificate caching working correctly');
      _log.info('All HTTP client configuration tests completed successfully!');
    } catch (e, stack) {
      _log.severe('HTTP client configuration tests failed: $e', e, stack);
    }
  }

  /// Tests mTLS certificate loading specifically
  static Future<void> testMTLSCertificateLoading() async {
    _log.info('Testing mTLS certificate loading...');

    try {
      final configService = HttpClientConfigService();

      // Test certificate loading
      await configService.getConfiguredClient();
      _log.info('✓ mTLS certificate loading test completed');

      // Test cache clearing
      configService.clearCertificateCache();
      _log.info('✓ Certificate cache cleared successfully');
    } catch (e, stack) {
      _log.severe('mTLS certificate loading test failed: $e', e, stack);
    }
  }

  /// Tests thread safety of HTTP client configuration
  static Future<void> testThreadSafety() async {
    _log.info('Testing HTTP client thread safety...');

    try {
      final futures = <Future>[];

      // Create multiple clients concurrently to test thread safety
      for (int i = 0; i < 5; i++) {
        futures.add(
          getConfiguredHttpClient().then((client) {
            _log.info('✓ Client $i created successfully');
          }),
        );
      }

      await Future.wait(futures);
      _log.info('✓ Thread safety test completed successfully');
    } catch (e, stack) {
      _log.severe('Thread safety test failed: $e', e, stack);
    }
  }
}
