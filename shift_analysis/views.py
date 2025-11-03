from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import get_object_or_404
from .models import Shift, PredefinedFilter, FilterSchedule
from .serializers import (
    ShiftSerializer, 
    PredefinedFilterSerializer, 
    PredefinedFilterWithSchedulesSerializer,
    FilterScheduleSerializer
)


class ShiftListCreateView(APIView):
    """List all shifts or create a new shift"""
    
    def get(self, request):
        """Get all shifts with optional filtering"""
        queryset = Shift.objects.all()
        
        # Filter by is_active if provided
        is_active = request.query_params.get('is_active')
        if is_active is not None:
            queryset = queryset.filter(is_active=is_active.lower() == 'true')

        # Filter by should_transcribe if provided
        should_transcribe = request.query_params.get('should_transcribe')
        if should_transcribe is not None:
            queryset = queryset.filter(should_transcribe=should_transcribe.lower() == 'true')
        
        # Filter by channel if provided
        channel_id = request.query_params.get('channel')
        if channel_id:
            try:
                queryset = queryset.filter(channel_id=int(channel_id))
            except ValueError:
                return Response(
                    {'error': 'channel must be a valid integer'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        # Filter by days if provided
        days = request.query_params.get('days')
        if days:
            # Split comma-separated days and filter
            day_list = [day.strip().lower() for day in days.split(',')]
            # Use __icontains to match any of the specified days
            from django.db.models import Q
            day_filters = Q()
            for day in day_list:
                day_filters |= Q(days__icontains=day)
            queryset = queryset.filter(day_filters)
        
        # Filter by single day if provided (alternative to days parameter)
        day = request.query_params.get('day')
        if day:
            # Validate day parameter
            valid_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            if day.lower() not in valid_days:
                return Response(
                    {'error': f"Invalid day. Valid days are: {', '.join(valid_days)}"}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
            queryset = queryset.filter(days__icontains=day.lower())
        
        # Filter by flag_seconds range if provided
        min_flag_seconds = request.query_params.get('min_flag_seconds')
        if min_flag_seconds:
            try:
                queryset = queryset.filter(flag_seconds__gte=int(min_flag_seconds))
            except ValueError:
                return Response(
                    {'error': 'min_flag_seconds must be a valid integer'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        max_flag_seconds = request.query_params.get('max_flag_seconds')
        if max_flag_seconds:
            try:
                queryset = queryset.filter(flag_seconds__lte=int(max_flag_seconds))
            except ValueError:
                return Response(
                    {'error': 'max_flag_seconds must be a valid integer'}, 
                    status=status.HTTP_400_BAD_REQUEST
                )
        
        # Search by name or description
        search = request.query_params.get('search')
        if search:
            queryset = queryset.filter(name__icontains=search) | queryset.filter(description__icontains=search)
        
        # Order by field
        ordering = request.query_params.get('ordering', 'start_time')
        queryset = queryset.order_by(ordering)
        
        serializer = ShiftSerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def post(self, request):
        """Create a new shift"""
        serializer = ShiftSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class ShiftDetailView(APIView):
    """Retrieve, update or delete a shift"""
    
    def get(self, request, pk):
        """Get a specific shift"""
        shift = get_object_or_404(Shift, pk=pk)
        serializer = ShiftSerializer(shift)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def put(self, request, pk):
        """Update a shift"""
        shift = get_object_or_404(Shift, pk=pk)
        serializer = ShiftSerializer(shift, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def patch(self, request, pk):
        """Partially update a shift"""
        shift = get_object_or_404(Shift, pk=pk)
        serializer = ShiftSerializer(shift, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def delete(self, request, pk):
        """Delete a shift"""
        shift = get_object_or_404(Shift, pk=pk)
        shift.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class ActiveShiftsView(APIView):
    """Get only active shifts"""
    
    def get(self, request):
        """Get all active shifts"""
        shifts = Shift.objects.filter(is_active=True)

        # Optional filter: should_transcribe=true|false
        should_transcribe = request.query_params.get('should_transcribe')
        if should_transcribe is not None:
            shifts = shifts.filter(should_transcribe=should_transcribe.lower() == 'true')

        shifts = shifts.order_by('start_time')
        serializer = ShiftSerializer(shifts, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class PredefinedFilterListCreateView(APIView):
    """List all predefined filters or create a new one"""
    
    def get(self, request):
        """Get all predefined filters with optional filtering"""
        queryset = PredefinedFilter.objects.all()
        
        # Filter by is_active if provided
        is_active = request.query_params.get('is_active')
        if is_active is not None:
            queryset = queryset.filter(is_active=is_active.lower() == 'true')
        
        # Filter by channel if provided
        channel_id = request.query_params.get('channel')
        if channel_id:
            queryset = queryset.filter(channel_id=channel_id)
        
        # Filter by created_by if provided (now a foreign key)
        created_by = request.query_params.get('created_by')
        if created_by:
            from django.db.models import Q
            queryset = queryset.filter(
                Q(created_by__name__icontains=created_by) | 
                Q(created_by__email__icontains=created_by)
            )
        
        # Search by name or description
        search = request.query_params.get('search')
        if search:
            queryset = queryset.filter(name__icontains=search) | queryset.filter(description__icontains=search)
        
        # Order by field
        ordering = request.query_params.get('ordering', 'name')
        queryset = queryset.order_by(ordering)
        
        serializer = PredefinedFilterSerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def post(self, request):
        """Create a new predefined filter with schedules"""
        serializer = PredefinedFilterWithSchedulesSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class PredefinedFilterDetailView(APIView):
    """Retrieve, update or delete a predefined filter"""
    
    def get(self, request, pk):
        """Get a specific predefined filter"""
        predefined_filter = get_object_or_404(PredefinedFilter, pk=pk)
        serializer = PredefinedFilterSerializer(predefined_filter)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def put(self, request, pk):
        """Update a predefined filter"""
        predefined_filter = get_object_or_404(PredefinedFilter, pk=pk)
        serializer = PredefinedFilterWithSchedulesSerializer(predefined_filter, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def patch(self, request, pk):
        """Partially update a predefined filter"""
        predefined_filter = get_object_or_404(PredefinedFilter, pk=pk)
        serializer = PredefinedFilterWithSchedulesSerializer(predefined_filter, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def delete(self, request, pk):
        """Delete a predefined filter"""
        predefined_filter = get_object_or_404(PredefinedFilter, pk=pk)
        predefined_filter.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)



class PredefinedFilterSchedulesView(APIView):
    """Get all schedules for a specific predefined filter"""
    
    def get(self, request, pk):
        """Get schedules for a predefined filter"""
        predefined_filter = get_object_or_404(PredefinedFilter, pk=pk)
        schedules = predefined_filter.schedules.all().order_by('start_time')
        serializer = FilterScheduleSerializer(schedules, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)


class FilterScheduleListCreateView(APIView):
    """List all filter schedules or create a new one"""
    
    def get(self, request):
        """Get all filter schedules with optional filtering"""
        queryset = FilterSchedule.objects.all()
        
        # Filter by predefined_filter if provided
        filter_id = request.query_params.get('predefined_filter')
        if filter_id:
            queryset = queryset.filter(predefined_filter_id=filter_id)
        
        # Search by predefined filter name or notes
        search = request.query_params.get('search')
        if search:
            queryset = queryset.filter(predefined_filter__name__icontains=search) | queryset.filter(notes__icontains=search)
        
        # Order by field
        ordering = request.query_params.get('ordering', 'start_time')
        queryset = queryset.order_by(ordering)
        
        serializer = FilterScheduleSerializer(queryset, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def post(self, request):
        """Create a new filter schedule"""
        serializer = FilterScheduleSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class FilterScheduleDetailView(APIView):
    """Retrieve, update or delete a filter schedule"""
    
    def get(self, request, pk):
        """Get a specific filter schedule"""
        schedule = get_object_or_404(FilterSchedule, pk=pk)
        serializer = FilterScheduleSerializer(schedule)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def put(self, request, pk):
        """Update a filter schedule"""
        schedule = get_object_or_404(FilterSchedule, pk=pk)
        serializer = FilterScheduleSerializer(schedule, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def patch(self, request, pk):
        """Partially update a filter schedule"""
        schedule = get_object_or_404(FilterSchedule, pk=pk)
        serializer = FilterScheduleSerializer(schedule, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    
    def delete(self, request, pk):
        """Delete a filter schedule"""
        schedule = get_object_or_404(FilterSchedule, pk=pk)
        schedule.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)
