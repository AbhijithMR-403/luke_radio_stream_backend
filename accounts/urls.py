
from django.urls import path
from .views import (
    AdminCreateUserView, AdminUpdateUserView, AdminListUsersView, AdminAssignChannelView, AdminUnassignChannelView, UserChannelsView,
    VerifyMagicLinkView, SetPasswordView, LoginView, ResendMagicLinkView,
    CustomTokenObtainPairView, CustomTokenRefreshView
)


urlpatterns = [
	# Admin endpoints
	path('admin/create-user/', AdminCreateUserView.as_view(), name='admin-create-user'),
	path('admin/update-user/<int:user_id>/', AdminUpdateUserView.as_view(), name='admin-update-user'),
	path('admin/list-users/', AdminListUsersView.as_view(), name='admin-list-users'),
	path('admin/assign-channel/', AdminAssignChannelView.as_view(), name='admin-assign-channel'),
	path('admin/unassign-channel/', AdminUnassignChannelView.as_view(), name='admin-unassign-channel'),
	
	# User authentication
	path('auth/verify-magic-link/', VerifyMagicLinkView.as_view(), name='verify-magic-link'),
	path('auth/set-password/', SetPasswordView.as_view(), name='set-password'),
	path('auth/resend-magic-link/', ResendMagicLinkView.as_view(), name='resend-magic-link'),
    
    #**** Not used for now, remove later
	path('auth/login/', LoginView.as_view(), name='login'),
    
	# JWT Token
    path('token/', CustomTokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('token/refresh/', CustomTokenRefreshView.as_view(), name='token_refresh'),

	# User endpoints
	path('user/channels/', UserChannelsView.as_view(), name='user-channels'),
]
