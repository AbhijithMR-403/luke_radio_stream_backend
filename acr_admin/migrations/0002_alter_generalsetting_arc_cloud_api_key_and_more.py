# Generated by Django 5.2.4 on 2025-07-21 08:17

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("acr_admin", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="generalsetting",
            name="arc_cloud_api_key",
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name="generalsetting",
            name="openai_api_key",
            field=models.TextField(),
        ),
        migrations.AlterField(
            model_name="generalsetting",
            name="revai_access_token",
            field=models.TextField(),
        ),
    ]
