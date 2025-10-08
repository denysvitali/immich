import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:immich_mobile/domain/models/asset/base_asset.model.dart';
import 'package:immich_mobile/domain/services/timeline.service.dart';
import 'package:immich_mobile/entities/asset.entity.dart';
import 'package:immich_mobile/infrastructure/repositories/timeline.repository.dart';
import 'package:immich_mobile/presentation/widgets/timeline/timeline.state.dart';
import 'package:immich_mobile/providers/infrastructure/db.provider.dart';
import 'package:immich_mobile/providers/infrastructure/setting.provider.dart';
import 'package:immich_mobile/providers/user.provider.dart';
import 'package:immich_mobile/widgets/asset_grid/asset_grid_data_structure.dart';

final timelineRepositoryProvider = Provider<DriftTimelineRepository>(
  (ref) => DriftTimelineRepository(ref.watch(driftProvider)),
);

final timelineArgsProvider = Provider.autoDispose<TimelineArgs>(
  (ref) => throw UnimplementedError('Will be overridden through a ProviderScope.'),
);

final timelineServiceProvider = Provider<TimelineService>(
  (ref) {
    final timelineUsers = ref.watch(timelineUsersProvider).valueOrNull ?? [];
    final timelineService = ref.watch(timelineFactoryProvider).main(timelineUsers);
    ref.onDispose(timelineService.dispose);
    return timelineService;
  },
  // Empty dependencies to inform the framework that this provider
  // might be used in a ProviderScope
  dependencies: [],
);

final timelineFactoryProvider = Provider<TimelineFactory>(
  (ref) => TimelineFactory(
    timelineRepository: ref.watch(timelineRepositoryProvider),
    settingsService: ref.watch(settingsProvider),
  ),
);

final timelineUsersProvider = StreamProvider<List<String>>((ref) {
  final currentUserId = ref.watch(currentUserProvider.select((u) => u?.id));
  if (currentUserId == null) {
    return Stream.value([]);
  }

  return ref.watch(timelineRepositoryProvider).watchTimelineUserIds(currentUserId);
});

// Bridge provider to convert new Drift timeline service to old RenderList format
final homeTimelineProvider = StreamProvider<RenderList>((ref) {
  final timelineService = ref.watch(timelineServiceProvider);
  
  // Convert the new timeline service to RenderList format
  return timelineService.watchBuckets().asyncMap((buckets) async {
    final assets = <BaseAsset>[];
    
    // Load assets for each bucket
    for (final bucket in buckets) {
      final bucketAssets = await timelineService.loadAssets(0, bucket.assetCount);
      assets.addAll(bucketAssets);
    }
    
    // Convert to RenderList format
    // Note: This is a simplified conversion - in practice, you might need to handle the conversion differently
    return RenderList.fromAssets(assets.cast<Asset>(), GroupAssetsBy.day);
  });
});
