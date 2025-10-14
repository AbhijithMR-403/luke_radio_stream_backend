from rest_framework import serializers
from django.db import IntegrityError, transaction
from .models import Shift, PredefinedFilter, FilterSchedule
from acr_admin.models import Channel
from accounts.models import RadioUser


class ShiftSerializer(serializers.ModelSerializer):
    """Serializer for Shift model"""
    
    class Meta:
        model = Shift
        fields = '__all__'
        read_only_fields = ('created_at', 'updated_at')

    def validate(self, data):
        """Validate that start_time is before end_time"""
        if data.get('start_time') and data.get('end_time'):
            if data['start_time'] >= data['end_time']:
                raise serializers.ValidationError("Start time must be before end time")
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
        """Validate that start_time is before end_time"""
        if data.get('start_time') and data.get('end_time'):
            if data['start_time'] >= data['end_time']:
                raise serializers.ValidationError("Start time must be before end time")
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
