import 'package:immich_mobile/presentation/widgets/images/remote_image_provider.dart';
import 'package:flutter/material.dart';
import 'package:immich_mobile/domain/models/store.model.dart';
import 'package:immich_mobile/domain/models/user.model.dart';
import 'package:immich_mobile/entities/store.entity.dart';
import 'package:immich_mobile/extensions/build_context_extensions.dart';

class PartnerUserAvatar extends StatelessWidget {
  const PartnerUserAvatar({super.key, required this.partner});

  final PartnerUserDto partner;

  @override
  Widget build(BuildContext context) {
    final url = "${Store.get(StoreKey.serverEndpoint)}/users/${partner.id}/profile-image";
    final nameFirstLetter = partner.name.isNotEmpty ? partner.name[0] : "";
    return CircleAvatar(
      radius: 16,
      backgroundColor: context.primaryColor.withAlpha(50),
      foregroundImage: RemoteUrlImageProvider(url: url),
      // silence errors if user has no profile image, use initials as fallback
      onForegroundImageError: (exception, stackTrace) {},
      child: Text(nameFirstLetter.toUpperCase()),
    );
  }
}
