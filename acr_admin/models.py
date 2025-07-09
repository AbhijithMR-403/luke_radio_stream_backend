from django.db import models
from django.core.exceptions import ValidationError

# Create your models here.
class Channel(models.Model):
    name = models.CharField(max_length=255, blank=True)  # Optional label
    channel_id = models.CharField(max_length=255)
    project_id = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    is_deleted = models.BooleanField(default=False)  # To soft delete

    def __str__(self):
        return f"Channel {self.channel_id} in Project {self.project_id}"

class GeneralSetting(models.Model):

    # Auth Keys
    openai_api_key = models.CharField(max_length=255)
    openai_org_id = models.CharField(max_length=255)
    arc_cloud_api_key = models.CharField(max_length=255)
    revai_access_token = models.CharField(max_length=255)

    # Prompts
    summarize_transcript_prompt = models.TextField()
    sentiment_analysis_prompt = models.TextField()
    general_topics_prompt = models.TextField()
    iab_topics_prompt = models.TextField()


    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Settings: {self.heading}"


class WellnessBucket(models.Model):
    bucket_id = models.CharField(max_length=20, unique=True, editable=False)  # Auto "bucket_1", "bucket_2", ...
    title = models.CharField(max_length=255)  # eg: "Emotional Wellness"
    description = models.TextField()
    prompt = models.TextField(help_text="Prompt to use when analyzing transcript for this bucket")

    def __str__(self):
        return f"{self.bucket_id} - {self.title}"

    def clean(self):
        if not self.pk and WellnessBucket.objects.count() >= 20:
            raise ValidationError("You cannot have more than 20 wellness buckets.")

    def save(self, *args, **kwargs):
        import re
        # If bucket_id is provided, validate it
        if self.bucket_id:
            print(self.bucket_id)
            match = re.match(r'^bucket_(\d{1,2})$', self.bucket_id)
            print(match)
            if not match:
                raise ValidationError("bucket_id must be in the format 'bucket_N' where N is 1-20.")
            number = int(match.group(1))
            if not (1 <= number <= 20):
                raise ValidationError("bucket_id must be between bucket_1 and bucket_20.")
            # Ensure uniqueness is handled by the model's unique constraint
        else:
            # Auto-generate next available bucket_id
            existing = WellnessBucket.objects.all().order_by('bucket_id')
            print(existing)
            existing_ids = {
                int(bucket.bucket_id.split('_')[1])
                for bucket in existing if bucket.bucket_id.startswith("bucket_")
            }
            print("---**-----")
            print(existing_ids)
            for i in range(1, 21):
                if i not in existing_ids:
                    self.bucket_id = f"bucket_{i}"
                    break
            else:
                raise ValidationError("Max 20 buckets already created.")
        super().save(*args, **kwargs)

class BucketSentence(models.Model):
    bucket = models.ForeignKey(WellnessBucket, on_delete=models.CASCADE, related_name="sentences")
    content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Sentence for {self.bucket.keyword}"
