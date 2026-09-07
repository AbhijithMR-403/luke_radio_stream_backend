from rest_framework import permissions


class IsAdminUser(permissions.BasePermission):
    """Only authenticated users flagged as admin (RadioUser.is_admin)."""

    def has_permission(self, request, view):
        return bool(request.user and request.user.is_authenticated and request.user.is_admin)
