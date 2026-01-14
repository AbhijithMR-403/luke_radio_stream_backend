from django.apps import AppConfig


# class AcrConfig(AppConfig):
#     default_auto_field = "django.db.models.BigAutoField"
#     name = "core_admin"
#     label = "acr_admin"

# Core Admin - renamed from AcrConfig to CoreAdminConfig
# previously folder name was acr_admin
class CoreAdminConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "core_admin"
    label = "acr_admin"
