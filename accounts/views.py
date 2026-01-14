# Create your views here.

from rest_framework import generics, permissions, status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
from rest_framework_simplejwt.exceptions import InvalidToken, TokenError
from django.contrib.auth import authenticate
from django.utils import timezone
from .models import RadioUser, UserChannelAssignment, MagicLink
from .serializer import (
    UserSerializer, UserChannelAssignmentSerializer, AssignChannelSerializer,
    AdminCreateUserSerializer, AdminUpdateUserSerializer, MagicLinkVerificationSerializer, PasswordSetupSerializer,
    CustomTokenObtainPairSerializer, ChannelSerializer
)
from .utils import generate_and_send_magic_link
from core_admin.models import Channel
from django.contrib.auth import get_user_model

User = get_user_model()

# Permission: Only admin users
class IsAdminUser(permissions.BasePermission):
    def has_permission(self, request, view):
        return request.user.is_authenticated and request.user.is_admin

# Admin: Create user (only email required)
class AdminCreateUserView(APIView):
    permission_classes = [IsAdminUser]

    def post(self, request):
        serializer = AdminCreateUserSerializer(data=request.data)
        if serializer.is_valid():
            email = serializer.validated_data['email']
            name = serializer.validated_data['name']
            
            # Check if user already exists
            if User.objects.filter(email=email).exists():
                return Response({'error': 'User with this email already exists.'}, status=status.HTTP_400_BAD_REQUEST)
            
            # Create user without password
            user = User.objects.create_user(email=email, name=name)
            
            # Generate and send magic link
            magic_link = generate_and_send_magic_link(user)
            if magic_link:
                return Response({
                    'message': 'User created successfully. Magic link sent to email.',
                    'user': UserSerializer(user).data
                }, status=status.HTTP_201_CREATED)
            else:
                return Response({
                    'error': 'User created but failed to send magic link email.',
                    'user': UserSerializer(user).data
                }, status=status.HTTP_201_CREATED)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# Admin: Update user information
class AdminUpdateUserView(APIView):
    permission_classes = [IsAdminUser]

    def put(self, request, user_id):
        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found.'}, status=status.HTTP_404_NOT_FOUND)
        
        serializer = AdminUpdateUserSerializer(data=request.data)
        if serializer.is_valid():
            
            if 'name' in serializer.validated_data:
                user.name = serializer.validated_data['name']
            
            if 'is_active' in serializer.validated_data:
                user.is_active = serializer.validated_data['is_active']
            
            user.save()
            
            return Response({
                'message': 'User updated successfully.',
                'user': UserSerializer(user).data
            }, status=status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# Admin: List all users
class AdminListUsersView(APIView):
    permission_classes = [IsAdminUser]

    def get(self, request):
        users = User.objects.all().order_by('-id')
        serializer = UserSerializer(users, many=True)
        return Response(serializer.data)

# Admin: Delete user
class AdminDeleteUserView(APIView):
    permission_classes = [IsAdminUser]

    def delete(self, request, user_id):
        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found.'}, status=status.HTTP_404_NOT_FOUND)
        
        # Prevent admin from deleting themselves
        if user.id == request.user.id:
            return Response({'error': 'You cannot delete your own account.'}, status=status.HTTP_400_BAD_REQUEST)
        
        # Store user email for response before deletion
        user_email = user.email
        
        # Delete the user (related objects will be deleted via CASCADE)
        user.delete()
        
        return Response({
            'message': f'User {user_email} deleted successfully.'
        }, status=status.HTTP_200_OK)

# Admin: Assign channel to user
class AdminAssignChannelView(APIView):
    permission_classes = [IsAdminUser]

    def post(self, request):
        serializer = AssignChannelSerializer(data=request.data)
        if serializer.is_valid():
            user_id = serializer.validated_data['user_id']
            channel_id = serializer.validated_data['channel_id']
            try:
                user = User.objects.get(id=user_id)
                channel = Channel.objects.get(id=channel_id)
            except (User.DoesNotExist, Channel.DoesNotExist):
                return Response({'error': 'User or Channel not found.'}, status=status.HTTP_404_NOT_FOUND)
            assignment, created = UserChannelAssignment.objects.get_or_create(
                user=user,
                channel=channel,
                defaults={'assigned_by': request.user}
            )
            return Response({
                'assigned': created,
                'already_exists': not created,
                'message': 'Channel assigned to user.' if created else 'Assignment already exists.',
                'user': UserSerializer(user).data
            }, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# Admin: Unassign channel from user
class AdminUnassignChannelView(APIView):
    permission_classes = [IsAdminUser]

    def post(self, request):
        serializer = AssignChannelSerializer(data=request.data)
        if serializer.is_valid():
            user_id = serializer.validated_data['user_id']
            channel_id = serializer.validated_data['channel_id']
            try:
                user = User.objects.get(id=user_id)
                channel = Channel.objects.get(id=channel_id)
            except (User.DoesNotExist, Channel.DoesNotExist):
                return Response({'error': 'User or Channel not found.'}, status=status.HTTP_404_NOT_FOUND)
            
            try:
                assignment = UserChannelAssignment.objects.get(user=user, channel=channel)
                assignment.delete()
                return Response({
                    'unassigned': True,
                    'message': 'Channel unassigned from user.',
                    'user': UserSerializer(user).data
                }, status=status.HTTP_200_OK)
            except UserChannelAssignment.DoesNotExist:
                return Response({
                    'unassigned': False,
                    'message': 'Assignment does not exist.',
                    'user': UserSerializer(user).data
                }, status=status.HTTP_404_NOT_FOUND)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# User: View assigned channels
class UserChannelsView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        user_data = UserSerializer(request.user).data
        if request.user.is_admin:
            channels = Channel.objects.filter(is_deleted=False)
            channels_data = ChannelSerializer(channels, many=True).data
            return Response({
                'user': user_data,
                'channels': channels_data,
                'is_admin': True
            })
        else:
            assignments = UserChannelAssignment.objects.filter(user=request.user).select_related('channel')
            channels = [assignment.channel for assignment in assignments]
            channels_data = ChannelSerializer(channels, many=True).data
            return Response({
                'user': user_data,
                'channels': channels_data,
                'is_admin': False
            })

# User: Verify magic link token
class VerifyMagicLinkView(APIView):
    def post(self, request):
        serializer = MagicLinkVerificationSerializer(data=request.data)
        if serializer.is_valid():
            token = serializer.validated_data['token']
            
            # Find valid magic link
            magic_link = MagicLink.objects.filter(
                token=token,
                is_used=False
            ).first()
            
            if not magic_link or not magic_link.is_valid():
                return Response({'error': 'Invalid or expired magic link.', 'verify': False}, status=status.HTTP_400_BAD_REQUEST)
            
            return Response({
                'message': 'Magic link verified successfully. You can now set your password.',
                'user': UserSerializer(magic_link.user).data,
                'verify': True
            }, status=status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# User: Set password using magic link
class SetPasswordView(APIView):
    def post(self, request):
        serializer = PasswordSetupSerializer(data=request.data)
        if serializer.is_valid():
            token = serializer.validated_data['token']
            password = serializer.validated_data['password']
            
            # Find valid magic link
            magic_link = MagicLink.objects.filter(
                token=token,
                is_used=False
            ).first()
            
            if not magic_link or not magic_link.is_valid():
                return Response({'error': 'Invalid or expired magic link.'}, status=status.HTTP_400_BAD_REQUEST)
            
            # Mark magic link as used
            magic_link.mark_as_used()
            
            # Set password
            user = magic_link.user
            user.set_password(password)
            user.password_set = True
            user.save()
            
            return Response({
                'message': 'Password set successfully. You can now login.',
                'user': UserSerializer(user).data
            }, status=status.HTTP_200_OK)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# User: Resend magic link
class ResendMagicLinkView(APIView):
    def post(self, request):
        email = request.data.get('email')
        if not email:
            return Response({'error': 'Email is required.'}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist:
            return Response({'error': 'User not found.'}, status=status.HTTP_404_NOT_FOUND)
        
        # Check if a magic link was recently sent (within last 60 seconds)
        recent_magic_link = MagicLink.objects.filter(
            user=user
        ).order_by('-created_at').first()
        
        if recent_magic_link:
            time_since_sent = timezone.now() - recent_magic_link.created_at
            time_gap_seconds = 120  # 60 seconds gap between email sends
            
            if time_since_sent.total_seconds() < time_gap_seconds:
                remaining_seconds = int(time_gap_seconds - time_since_sent.total_seconds())
                return Response({
                    'error': f'Please wait {remaining_seconds} more seconds before requesting a new magic link.',
                    'seconds_remaining': remaining_seconds
                }, status=status.HTTP_429_TOO_MANY_REQUESTS)
        
        # Generate and send new magic link
        magic_link = generate_and_send_magic_link(user)
        if magic_link:
            return Response({'message': 'New magic link sent to your email.'}, status=status.HTTP_200_OK)
        else:
            return Response({'error': 'Failed to send magic link email.'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Custom JWT Token Views
class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer

    def post(self, request, *args, **kwargs):
        data = request.data.copy()
        email = data.get('email', '').strip()
        password = data.get('password', '')

        try:
            user = User.objects.get(email__iexact=email)
        except User.DoesNotExist:
            return Response({'error': 'Email not found.'}, status=status.HTTP_404_NOT_FOUND)

        if not user.is_active:
            return Response({'error': 'Account is inactive. Please contact your administrator.'}, status=status.HTTP_403_FORBIDDEN)

        if not user.password_set:
            return Response({'error': 'Account setup incomplete. Please set your password via the magic link.'}, status=status.HTTP_400_BAD_REQUEST)

        if not user.check_password(password):
            return Response({'error': 'Invalid credentials.'}, status=status.HTTP_401_UNAUTHORIZED)

        data['email'] = user.email

        serializer = self.get_serializer(data=data)
        try:
            serializer.is_valid(raise_exception=True)
        except TokenError as e:
            raise InvalidToken(e.args[0])

        return Response(serializer.validated_data, status=status.HTTP_200_OK)


class CustomTokenRefreshView(TokenRefreshView):
    def post(self, request, *args, **kwargs):
        response = super().post(request, *args, **kwargs)
        if response.status_code == 200:
            # Get user from the token
            from rest_framework_simplejwt.tokens import RefreshToken
            try:
                refresh_token = request.data.get('refresh')
                if refresh_token:
                    token = RefreshToken(refresh_token)
                    user_id = token.payload.get('user_id')
                    if user_id:
                        user = User.objects.get(id=user_id)
                        response.data['user'] = UserSerializer(user).data
            except Exception:
                pass  # If we can't get user info, just return the token
        return response
