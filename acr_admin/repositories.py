from django.db import transaction
from django.db.models import Max
from django.core.exceptions import ValidationError

from .models import GeneralSetting, WellnessBucket


class GeneralSettingService:

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

        # 🔒 Lock the table rows to prevent concurrent writes
        active_setting = (
            GeneralSetting.objects
            .select_for_update()
            .filter(is_active=True)
            .first()
        )
        print(active_setting.version, active_setting.is_active)

        # Lock all rows to prevent concurrent version creation
        GeneralSetting.objects.select_for_update().values("id")
        print(active_setting.version, active_setting.is_active)

        max_version = GeneralSetting.objects.aggregate(
            max_version=Max("version")
        )["max_version"] or 0

        next_version = max_version + 1
        print(active_setting.version, active_setting.is_active)

        # Exclude fields that shouldn't be set during creation
        # These are either auto-generated or explicitly set below
        excluded_fields = {
            'id', 'version', 'is_active', 'created_at', 'created_by', 
            'parent_version', 'change_reason'
        }
        filtered_settings_data = {
            k: v for k, v in settings_data.items() 
            if k not in excluded_fields
        }
        print(active_setting.version, active_setting.is_active)

        # Create new settings version (inactive for now)
        new_setting = GeneralSetting.objects.create(
            **filtered_settings_data,
            version=next_version,
            is_active=False,
            created_by=user,
            change_reason=change_reason,
            parent_version=active_setting,
        )
        print(active_setting.version, active_setting.is_active)

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
