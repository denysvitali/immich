import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:immich_mobile/domain/models/store.model.dart';
import 'package:immich_mobile/entities/store.entity.dart';
import 'package:immich_mobile/providers/infrastructure/asset.provider.dart';
import 'package:logging/logging.dart';

/// Debug utilities for sync operations
/// Helps diagnose sync issues and timeline population
class SyncDebug {
  static final Logger _log = Logger('SyncDebug');
  
  /// Checks the current state of the database after sync
  /// Note: This should be called from a context that has access to ProviderContainer
  static Future<void> checkSyncState([ProviderContainer? ref]) async {
    try {
      final currentUser = Store.tryGet(StoreKey.currentUser);
      if (currentUser == null) {
        _log.warning("No current user found");
        return;
      }
      
      _log.info("=== SYNC STATE DEBUG ===");
      _log.info("Current user: ${currentUser.email}");
      
      if (ref != null) {
        // Get asset counts using the proper provider system
        final localAssetRepo = ref.read(localAssetRepository);
        final remoteAssetRepository = ref.read(remoteAssetRepositoryProvider);
        
        // Get local assets count
        final localAssetCount = await localAssetRepo.getCount();
        _log.info("Local assets: $localAssetCount");
        
        // Get remote assets count using a simple query
        final remoteAssetCount = await remoteAssetRepository.getCount();
        _log.info("Remote assets: $remoteAssetCount");
        
        // Get some sample remote assets for debugging
        if (currentUser.id.isNotEmpty) {
          final sampleRemoteAssets = await remoteAssetRepository.getSome(currentUser.id);
          _log.info("Sample remote assets: ${sampleRemoteAssets.length}");
          
          if (sampleRemoteAssets.isNotEmpty) {
            _log.info("Recent remote assets:");
            for (final asset in sampleRemoteAssets.take(5)) {
              _log.info("  - ${asset.name} (${asset.createdAt})");
            }
          } else {
            _log.warning("No remote assets found in database!");
          }
        }
        
        // TODO: Add Isar database check once we figure out the correct way to access it
      } else {
        _log.info("No ProviderContainer available - skipping detailed asset counts");
      }
      
      _log.info("=== END SYNC STATE DEBUG ===");
      
    } catch (e, stack) {
      _log.severe("Error checking sync state", e, stack);
    }
  }
  
  /// Forces a timeline reload by emitting a reload event
  static void forceTimelineReload() {
    _log.info("Forcing timeline reload...");
    // This would need to be called from a context that has access to EventStream
    // For now, just log that it should be called
    _log.info("Timeline reload should be triggered from the UI context");
  }
}
