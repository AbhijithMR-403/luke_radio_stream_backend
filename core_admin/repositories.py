from django.db import transaction
from django.db.models import Max, Prefetch, Q
from django.core.exceptions import ValidationError

from .models import Channel, GeneralSetting, WellnessBucket


class GeneralSettingService:

    @staticmethod
    def get_active_setting(
        channel: Channel | int,
        include_buckets: bool = True,
        exclude_deleted_buckets: bool = True,
    ):
        """
        Internal method to fetch the active GeneralSetting row with optional bucket prefetching.
        
        Args:
            channel: Channel instance or channel ID (required). Filters settings by channel.
            include_buckets: If True, prefetches related wellness_buckets. Defaults to True.
            exclude_deleted_buckets: If True, excludes deleted buckets (is_deleted=True).
                                    Only applies when include_buckets=True. Defaults to True.
        
        Returns:
            GeneralSetting instance if found, None otherwise.
            If include_buckets=True, buckets are accessible via instance.wellness_buckets.all()
        """
        queryset = GeneralSetting.objects.filter(channel=channel, is_active=True)
        
        # Prefetch buckets with optional filtering
        if include_buckets:
            bucket_queryset = WellnessBucket.objects.all()
            
            if exclude_deleted_buckets:
                bucket_queryset = bucket_queryset.filter(is_deleted=False)
            
            queryset = queryset.prefetch_related(
                Prefetch('wellness_buckets', queryset=bucket_queryset)
            )
        
        return queryset.first()

    @staticmethod
    @transaction.atomic
    def create_new_version(
        *,
        settings_data: dict,
        buckets_data: list,
        user=None,
        change_reason: str | None = None
    ):
        """
        Creates a new GeneralSetting version atomically.
        Guarantees:
        - exactly one active version
        - no race conditions
        """

        # Determine channel scope for versioning and locking
        channel_id = settings_data.get("channel_id")

        # Resolve and attach channel instance (required by GeneralSetting.channel FK)
        channel_instance = None
        if channel_id is not None:
            try:
                channel_instance = Channel.objects.get(pk=channel_id)
                settings_data["channel"] = channel_instance
            except Channel.DoesNotExist:
                raise ValidationError(f"Channel with id {channel_id} does not exist")            

        # 🔒 Lock all existing settings rows for this channel (or all channels if None)
        existing_settings = list(GeneralSetting.objects.select_for_update().filter(channel=channel_id))


        # Find currently active setting (if any) among locked rows
        active_setting = next((s for s in existing_settings if s.is_active), None)

        # Compute max version within the locked scope
        max_version = max((s.version for s in existing_settings), default=0)

        next_version = max_version + 1

        # Exclude fields that shouldn't be set during creation
        excluded_fields = {
            'id',
            'version',
            'is_active',
            'created_at',
            'created_by',
            'parent_version',
            'change_reason',
            # We always pass the FK via `channel` (instance), not `channel_id`
            'channel_id',
        }
        filtered_settings_data = {
            k: v for k, v in settings_data.items() 
            if k not in excluded_fields
        }

        # Create new settings version (inactive for now)
        new_setting = GeneralSetting.objects.create(
            **filtered_settings_data,
            version=next_version,
            is_active=False,
            created_by=user,
            change_reason=change_reason,
            parent_version=active_setting,
        )

        # ---- Clone buckets from active version ----
        if active_setting:
            old_buckets = active_setting.wellness_buckets.filter(is_deleted=False)
            bucket_map = {}  # old_id → new_bucket

            for old_bucket in old_buckets:
                new_bucket = WellnessBucket.objects.create(
                    title=old_bucket.title,
                    description=old_bucket.description,
                    category=old_bucket.category,
                    general_setting=new_setting,
                    source_bucket_id=old_bucket,
                )
                bucket_map[old_bucket.id] = new_bucket

        else:
            bucket_map = {}

        # ---- Apply incoming bucket updates ----
        for bucket in buckets_data:
            bucket_id = bucket.get("id")

            if bucket_id:
                if bucket_id not in bucket_map:
                    raise ValidationError(
                        f"Bucket {bucket_id} does not exist in active version or was already deleted"
                    )

                target_bucket = bucket_map[bucket_id]

                if bucket.get("is_deleted") is True:
                    target_bucket.is_deleted = True
                else:
                    target_bucket.title = bucket.get("title", target_bucket.title)
                    target_bucket.description = bucket.get(
                        "description", target_bucket.description
                    )
                    
                    # Validate category if provided
                    if "category" in bucket:
                        valid_categories = [choice[0] for choice in WellnessBucket.CATEGORY_CHOICES]
                        if bucket["category"] not in valid_categories:
                            raise ValidationError(
                                f"Invalid category '{bucket['category']}'. Must be one of: {', '.join(valid_categories)}"
                            )
                        target_bucket.category = bucket["category"]

                target_bucket.save()

            else:
                # New bucket - validate required fields
                required_fields = ["title", "description", "category"]
                missing_fields = [field for field in required_fields if field not in bucket]
                if missing_fields:
                    raise ValidationError(
                        f"Missing required fields for new bucket: {', '.join(missing_fields)}"
                    )
                
                # Validate category choice
                valid_categories = [choice[0] for choice in WellnessBucket.CATEGORY_CHOICES]
                if bucket["category"] not in valid_categories:
                    raise ValidationError(
                        f"Invalid category '{bucket['category']}'. Must be one of: {', '.join(valid_categories)}"
                    )
                
                WellnessBucket.objects.create(
                    title=bucket["title"],
                    description=bucket["description"],
                    category=bucket["category"],
                    general_setting=new_setting,
                )

        # ---- Activate new version safely ----
        if active_setting:
            active_setting.is_active = False
            active_setting.save(update_fields=["is_active"])

        new_setting.is_active = True
        new_setting.save(update_fields=["is_active"])

        return new_setting

    @staticmethod
    @transaction.atomic
    def transfer_settings(source_channel_id, target_channel_id, user):
        """
        Deep copies the active settings of one channel to another.
        Creates a new version for the target channel, preserving
        version history and bucket audit trail.
        """
        if source_channel_id == target_channel_id:
            raise ValidationError("Source and target channels must be different.")

        # 1. Get the source active settings
        source_active = GeneralSetting.objects.filter(
            channel_id=source_channel_id,
            is_active=True,
        ).first()

        if not source_active:
            raise ValidationError(
                f"Source channel {source_channel_id} has no active settings to transfer."
            )

        # 2. Get the target active settings (if any)
        target_active = GeneralSetting.objects.filter(
            channel_id=target_channel_id,
            is_active=True,
        ).first()

        # 3. Convert the source object to a dictionary for our creation method
        # We exclude internal / versioning fields so the versioning logic handles them
        excluded = {
            "id",
            "version",
            "is_active",
            "created_at",
            "created_by",
            "parent_version",
            "change_reason",
            "channel",
        }

        settings_data = {
            f.name: getattr(source_active, f.name)
            for f in source_active._meta.concrete_fields
            if f.name not in excluded
        }

        # Explicitly set the target channel (used by create_new_version)
        settings_data["channel_id"] = target_channel_id

        # 4. Build bucket operations so that:
        #    - existing target buckets are soft-deleted
        #    - source buckets are recreated on the new version
        buckets_data = []

        # a) Mark existing target buckets as deleted in the new version
        if target_active:
            for bucket in target_active.wellness_buckets.filter(is_deleted=False):
                buckets_data.append(
                    {
                        "id": bucket.id,
                        "is_deleted": True,
                    }
                )

        # b) Add source buckets as brand new buckets
        for bucket in source_active.wellness_buckets.filter(is_deleted=False):
            buckets_data.append(
                {
                    "title": bucket.title,
                    "description": bucket.description,
                    "category": bucket.category,
                }
            )

        # 5. Reuse the existing robust versioning logic
        return GeneralSettingService.create_new_version(
            settings_data=settings_data,
            buckets_data=buckets_data,
            user=user,
            change_reason=f"Transferred from channel {source_channel_id} to {target_channel_id}",
        )

    @staticmethod
    @transaction.atomic
    def revert_to_version(channel_id, target_version_number, user):
        """
        Reverts settings for a channel to a specific historical version by
        creating a brand new version cloned from that historical target.
        """
        # 1. Fetch the historical version we want to copy
        historical_version = GeneralSetting.objects.filter(
            channel_id=channel_id,
            version=target_version_number,
        ).first()

        if not historical_version:
            raise ValidationError(
                f"Version {target_version_number} not found for channel {channel_id}."
            )

        # 2. Extract settings data from the historical record
        excluded = {
            "id",
            "version",
            "is_active",
            "created_at",
            "created_by",
            "parent_version",
            "change_reason",
        }
        settings_data = {
            f.name: getattr(historical_version, f.name)
            for f in historical_version._meta.concrete_fields
            if f.name not in excluded
        }
        settings_data["channel_id"] = channel_id

        # 3. Get the buckets as they were in that historical version
        historical_buckets = historical_version.wellness_buckets.filter(
            is_deleted=False
        )
        buckets_data = [
            {
                "title": b.title,
                "description": b.description,
                "category": b.category,
            }
            for b in historical_buckets
        ]

        # 4. Create a brand new version using the existing logic
        return GeneralSettingService.create_new_version(
            settings_data=settings_data,
            buckets_data=buckets_data,
            user=user,
            change_reason=f"Reverted to version {target_version_number}",
        )


