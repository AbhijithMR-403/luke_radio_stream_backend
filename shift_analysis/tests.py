from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APITestCase
from rest_framework import status
from .models import Shift, PredefinedFilter, FilterSchedule
from datetime import time


class ShiftAPITestCase(APITestCase):
    """Test cases for Shift API endpoints"""
    
    def setUp(self):
        """Set up test data"""
        self.shift_data = {
            'name': 'Morning Shift',
            'start_time': '09:00:00',
            'end_time': '17:00:00',
            'description': 'Regular morning shift',
            'is_active': True
        }
    
    def test_create_shift(self):
        """Test creating a new shift"""
        url = reverse('shift-list')
        response = self.client.post(url, self.shift_data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Shift.objects.count(), 1)
        self.assertEqual(Shift.objects.get().name, 'Morning Shift')
    
    def test_get_shifts(self):
        """Test retrieving shifts"""
        Shift.objects.create(**self.shift_data)
        url = reverse('shift-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)


class PredefinedFilterAPITestCase(APITestCase):
    """Test cases for PredefinedFilter API endpoints"""
    
    def setUp(self):
        """Set up test data"""
        self.filter_data = {
            'name': 'Weekday Filter',
            'description': 'Filter for weekdays',
            'days_of_week': ['monday', 'tuesday', 'wednesday', 'thursday', 'friday'],
            'is_active': True,
            'created_by': 'test_user'
        }
    
    def test_create_predefined_filter(self):
        """Test creating a new predefined filter"""
        url = reverse('predefined-filter-list')
        response = self.client.post(url, self.filter_data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(PredefinedFilter.objects.count(), 1)
        filter_obj = PredefinedFilter.objects.get()
        self.assertEqual(filter_obj.name, 'Weekday Filter')
        self.assertEqual(filter_obj.days_of_week, ['monday', 'tuesday', 'wednesday', 'thursday', 'friday'])
    
    def test_get_predefined_filters(self):
        """Test retrieving predefined filters"""
        PredefinedFilter.objects.create(**self.filter_data)
        url = reverse('predefined-filter-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertIn('days_of_week_display', response.data[0])


class FilterScheduleAPITestCase(APITestCase):
    """Test cases for FilterSchedule API endpoints"""
    
    def setUp(self):
        """Set up test data"""
        self.filter = PredefinedFilter.objects.create(
            name='Test Filter',
            days_of_week=['monday', 'tuesday'],
            is_active=True
        )
        self.schedule_data = {
            'predefined_filter': self.filter.id,
            'start_time': '09:00:00',
            'end_time': '17:00:00',
            'notes': 'Test schedule'
        }
    
    def test_create_filter_schedule(self):
        """Test creating a new filter schedule"""
        url = reverse('filter-schedule-list')
        response = self.client.post(url, self.schedule_data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(FilterSchedule.objects.count(), 1)
        schedule = FilterSchedule.objects.get()
        self.assertEqual(schedule.predefined_filter, self.filter)
        self.assertEqual(str(schedule.start_time), '09:00:00')
    
    def test_get_filter_schedules(self):
        """Test retrieving filter schedules"""
        FilterSchedule.objects.create(**self.schedule_data)
        url = reverse('filter-schedule-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
