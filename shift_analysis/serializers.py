from rest_framework import serializers
from django.db import IntegrityError, transaction
from .models import Shift, PredefinedFilter, FilterSchedule
from acr_admin.models import Channel
from accounts.models import RadioUser


class ShiftSerializer(serializers.ModelSerializer):
    """Serializer for Shift model"""
    days_display = serializers.CharField(source='get_days_display', read_only=True)
    channel_name = serializers.CharField(source='channel.name', read_only=True)
    
    class Meta:
        model = Shift
        fields = '__all__'
        read_only_fields = ('created_at', 'updated_at')
        extra_kwargs = {
            'days': {'required': True},
            'channel': {'required': True}
        }
    
    def is_valid(self, raise_exception=False):
        """Override to convert non_field_errors to errors"""
        is_valid = super().is_valid(raise_exception=False)
        
        # Convert non_field_errors to errors
        if not is_valid and hasattr(self, '_errors') and 'non_field_errors' in self._errors:
            non_field_errors = self._errors.pop('non_field_errors')
            self._errors['errors'] = non_field_errors
        
        if not is_valid and raise_exception:
            raise serializers.ValidationError(self._errors)
        
        return is_valid

    def validate(self, data):
        """Validate shift data including days and times"""
        # Validate start and end times
        if data.get('start_time') and data.get('end_time'):
            if data['start_time'] == data['end_time']:
                raise serializers.ValidationError("Start and end time cannot be the same")
        
        # Validate days field
        days = data.get('days')
        if not days:
            raise serializers.ValidationError("At least one day must be specified")
        
        # Check if all specified days are valid
        valid_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        day_list = [day.strip().lower() for day in days.split(',')]
        
        for day in day_list:
            if day not in valid_days:
                raise serializers.ValidationError(f"Invalid day: {day}. Valid days are: {', '.join(valid_days)}")
        
        # Check for duplicate days
        if len(day_list) != len(set(day_list)):
            raise serializers.ValidationError("Duplicate days are not allowed")
        
         # Validate flag_seconds field
        flag_seconds = data.get('flag_seconds')
        if flag_seconds is not None and flag_seconds < 0:
            raise serializers.ValidationError("flag_seconds must be a non-negative integer")
        
        # Validate unique name per channel
        name = data.get('name')
        channel = data.get('channel')
        if name and channel:
            # Check if a shift with the same name and channel already exists
            existing_shift = Shift.objects.filter(name=name, channel=channel)
            # If updating, exclude current instance
            if self.instance:
                existing_shift = existing_shift.exclude(pk=self.instance.pk)
            
            if existing_shift.exists():
                raise serializers.ValidationError(
                    f"A shift with the name '{name}' already exists for this channel."
                )
        
        return data


class FilterScheduleSerializer(serializers.ModelSerializer):
    """Serializer for FilterSchedule model"""
    day_of_week_display = serializers.CharField(source='get_day_of_week_display', read_only=True)
    
    class Meta:
        model = FilterSchedule
        fields = '__all__'
        read_only_fields = ('created_at', 'updated_at')
        extra_kwargs = {
            # `predefined_filter` is provided by the parent `PredefinedFilterWithSchedulesSerializer`
            # during create/update, so it should not be required in nested input.
            'predefined_filter': {'required': False, 'read_only': True},
        }

    def validate(self, data):
        """Allow overnight ranges; disallow identical start/end (zero-length)."""
        if data.get('start_time') and data.get('end_time'):
            if data['start_time'] == data['end_time']:
                raise serializers.ValidationError("Start and end time cannot be the same")
        return data
    
    def validate_day_of_week(self, value):
        """Validate day_of_week field"""
        valid_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        if value not in valid_days:
            raise serializers.ValidationError(f"'{value}' is not a valid day of the week. Valid days are: {', '.join(valid_days)}")
        return value


class PredefinedFilterSerializer(serializers.ModelSerializer):
    """Serializer for PredefinedFilter model with nested schedules"""
    schedules = FilterScheduleSerializer(many=True, read_only=True)
    schedule_count = serializers.SerializerMethodField()
    channel_name = serializers.CharField(source='channel.name', read_only=True)
    created_by_name = serializers.CharField(source='created_by.name', read_only=True)
    created_by_email = serializers.EmailField(source='created_by.email', read_only=True)
    
    class Meta:
        model = PredefinedFilter
        fields = '__all__'
        read_only_fields = ('created_at', 'updated_at')
    
    def get_schedule_count(self, obj):
        """Get the number of schedules for this filter"""
        return obj.schedules.count()


class PredefinedFilterWithSchedulesSerializer(serializers.ModelSerializer):
    """Serializer for PredefinedFilter with all schedules included"""
    schedules = FilterScheduleSerializer(many=True)
    channel_name = serializers.CharField(source='channel.name', read_only=True)
    created_by_name = serializers.CharField(source='created_by.name', read_only=True)
    created_by_email = serializers.EmailField(source='created_by.email', read_only=True)
    
    class Meta:
        model = PredefinedFilter
        fields = '__all__'
        read_only_fields = ('created_at', 'updated_at')
    
    def create(self, validated_data):
        """Create PredefinedFilter with schedules"""
        schedules_data = validated_data.pop('schedules', [])

        # Validate duplicate schedule entries within incoming payload
        seen_combinations = set()
        for schedule in schedules_data:
            key = (
                schedule.get('day_of_week'),
                schedule.get('start_time'),
                schedule.get('end_time'),
            )
            if key in seen_combinations:
                raise serializers.ValidationError({
                    'schedules': [
                        f"Duplicate schedule for day_of_week={key[0]}, start_time={key[1]}, end_time={key[2]}"
                    ]
                })
            seen_combinations.add(key)

        try:
            with transaction.atomic():
                predefined_filter = PredefinedFilter.objects.create(**validated_data)
                for schedule_data in schedules_data:
                    FilterSchedule.objects.create(predefined_filter=predefined_filter, **schedule_data)
                return predefined_filter
        except IntegrityError:
            # Convert DB unique_together violation into a user-friendly validation error
            raise serializers.ValidationError({
                'schedules': [
                    'One or more schedules violate uniqueness: (day_of_week, start_time, end_time) must be unique per predefined filter.'
                ]
            })
    
    def update(self, instance, validated_data):
        """Update PredefinedFilter with schedules"""
        schedules_data = validated_data.pop('schedules', [])

        # Validate duplicate schedule entries within incoming payload
        seen_combinations = set()
        for schedule in schedules_data:
            key = (
                schedule.get('day_of_week'),
                schedule.get('start_time'),
                schedule.get('end_time'),
            )
            if key in seen_combinations:
                raise serializers.ValidationError({
                    'schedules': [
                        f"Duplicate schedule for day_of_week={key[0]}, start_time={key[1]}, end_time={key[2]}"
                    ]
                })
            seen_combinations.add(key)

        try:
            with transaction.atomic():
                # Update the filter instance
                for attr, value in validated_data.items():
                    setattr(instance, attr, value)
                instance.save()

                # Replace schedules
                instance.schedules.all().delete()
                for schedule_data in schedules_data:
                    FilterSchedule.objects.create(predefined_filter=instance, **schedule_data)
                return instance
        except IntegrityError:
            raise serializers.ValidationError({
                'schedules': [
                    'One or more schedules violate uniqueness: (day_of_week, start_time, end_time) must be unique per predefined filter.'
                ]
            })
