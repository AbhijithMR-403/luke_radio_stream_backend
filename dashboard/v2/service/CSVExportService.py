from typing import List, Dict, Optional
from datetime import datetime
from django.db.models import Q
from io import StringIO
import csv

from data_analysis.models import TranscriptionAnalysis
from shift_analysis.models import Shift
from dashboard.v2.service.BucketCountService import BucketCountService


class CSVExportService:
    """
    Service class for exporting transcription and analysis data to CSV
    """

    @staticmethod
    def get_transcription_analyses_for_export(
        start_dt: datetime,
        end_dt: datetime,
        channel_id: int = None,
        report_folder_id: Optional[int] = None,
        shift_id: Optional[int] = None
    ) -> List[TranscriptionAnalysis]:
        """
        Get TranscriptionAnalysis records filtered by date range, channel, active status, and optional shift.
        
        Args:
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            channel_id: Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id: Report folder ID to filter by (required if channel_id not provided)
            shift_id: Optional shift ID to filter by
        
        Returns:
            QuerySet of TranscriptionAnalysis records with active audio segments, ordered by start_time
        """
        # Validate filter inputs
        if channel_id is None and report_folder_id is None:
            raise ValueError("Either channel_id or report_folder_id must be provided")
        
        # Handle report folder case - get channel_id from folder
        if report_folder_id is not None:
            from data_analysis.models import ReportFolder
            try:
                report_folder = ReportFolder.objects.select_related('channel').get(id=report_folder_id)
                channel_id = report_folder.channel.id
            except ReportFolder.DoesNotExist:
                # If report folder doesn't exist, return empty queryset
                return TranscriptionAnalysis.objects.none()
        
        # Build Q object for filtering by date range, channel, and active status
        base_q = Q(
            transcription_detail__audio_segment__start_time__gte=start_dt,
            transcription_detail__audio_segment__start_time__lte=end_dt,
            transcription_detail__audio_segment__channel_id=channel_id,
            transcription_detail__audio_segment__is_active=True
        )
        
        # Add report_folder_id filter if provided
        if report_folder_id is not None:
            base_q &= Q(transcription_detail__audio_segment__saved_in_folders__folder_id=report_folder_id)
        
        # Apply shift filtering if shift_id is provided
        if shift_id is not None:
            try:
                shift = Shift.objects.get(id=shift_id, channel_id=channel_id)
                # Get Q object from shift's get_datetime_filter method
                shift_q = shift.get_datetime_filter(utc_start=start_dt, utc_end=end_dt)
                # Convert Q object to work with TranscriptionAnalysis by prefixing field paths
                shift_q_modified = BucketCountService._convert_shift_q_for_transcription_analysis(shift_q)
                # Combine with base query
                base_q = base_q & shift_q_modified
            except Shift.DoesNotExist:
                # If shift doesn't exist, return empty queryset
                return TranscriptionAnalysis.objects.none()
        
        # Get all TranscriptionAnalysis records with related data
        analyses = TranscriptionAnalysis.objects.filter(
            base_q
        ).select_related(
            'transcription_detail',
            'transcription_detail__audio_segment',
            'transcription_detail__audio_segment__channel'
        )
        
        # Use distinct() when report_folder_id is used to avoid duplicates from the join
        if report_folder_id is not None:
            analyses = analyses.distinct()
        
        analyses = analyses.order_by('transcription_detail__audio_segment__start_time')
        
        return analyses

    @staticmethod
    def generate_csv_content(analyses: List[TranscriptionAnalysis]) -> str:
        """
        Generate CSV content from TranscriptionAnalysis records.
        
        Args:
            analyses: List or QuerySet of TranscriptionAnalysis records
        
        Returns:
            CSV content as string
        """
        # Create CSV in memory
        output = StringIO()
        writer = csv.writer(output)
        
        # Write CSV header
        writer.writerow([
            'Segment ID',
            'Start Time',
            'End Time',
            'Duration (seconds)',
            'Title',
            'Title Before',
            'Title After',
            'File Name',
            'Channel ID',
            'Channel Name',
            'Transcript',
            'Summary',
            'Sentiment',
            'General Topics',
            'IAB Topics',
            'Bucket Prompt',
            'Content Type Prompt',
            'Analysis Created At'
        ])
        
        # Write data rows
        for analysis in analyses:
            transcription_detail = analysis.transcription_detail
            audio_segment = transcription_detail.audio_segment if transcription_detail else None
            transcript = transcription_detail.transcript if transcription_detail else ''
            
            writer.writerow([
                audio_segment.id if audio_segment else '',
                audio_segment.start_time.isoformat() if audio_segment and audio_segment.start_time else '',
                audio_segment.end_time.isoformat() if audio_segment and audio_segment.end_time else '',
                audio_segment.duration_seconds if audio_segment else '',
                audio_segment.title if audio_segment else '',
                audio_segment.title_before if audio_segment else '',
                audio_segment.title_after if audio_segment else '',
                audio_segment.file_name if audio_segment else '',
                audio_segment.channel.id if audio_segment and audio_segment.channel else '',
                audio_segment.channel.name if audio_segment and audio_segment.channel else '',
                transcript or '',
                analysis.summary or '',
                analysis.sentiment or '',
                analysis.general_topics or '',
                analysis.iab_topics or '',
                analysis.bucket_prompt or '',
                analysis.content_type_prompt or '',
                analysis.created_at.isoformat() if analysis.created_at else ''
            ])
        
        return output.getvalue()

    @staticmethod
    def generate_csv_filename(
        channel_id: int,
        start_dt: datetime,
        end_dt: datetime,
        report_folder_id: Optional[int] = None,
        shift_id: Optional[int] = None
    ) -> str:
        """
        Generate a descriptive filename for the CSV export.
        
        Args:
            channel_id: Channel ID
            start_dt: Start datetime
            end_dt: End datetime
            report_folder_id: Optional report folder ID
            shift_id: Optional shift ID
        
        Returns:
            Filename string
        """
        start_str = start_dt.strftime('%Y%m%d_%H%M%S')
        end_str = end_dt.strftime('%Y%m%d_%H%M%S')
        
        if report_folder_id:
            if shift_id:
                filename = f'transcription_export_folder_{report_folder_id}_shift_{shift_id}_{start_str}_to_{end_str}.csv'
            else:
                filename = f'transcription_export_folder_{report_folder_id}_{start_str}_to_{end_str}.csv'
        elif shift_id:
            filename = f'transcription_export_{channel_id}_shift_{shift_id}_{start_str}_to_{end_str}.csv'
        else:
            filename = f'transcription_export_{channel_id}_{start_str}_to_{end_str}.csv'
        
        return filename

    @staticmethod
    def export_to_csv(
        start_dt: datetime,
        end_dt: datetime,
        channel_id: int = None,
        report_folder_id: Optional[int] = None,
        shift_id: Optional[int] = None
    ) -> Dict[str, str]:
        """
        Export transcription and analysis data to CSV.
        
        Args:
            start_dt: Start datetime (timezone-aware)
            end_dt: End datetime (timezone-aware)
            channel_id: Channel ID to filter by (required if report_folder_id not provided)
            report_folder_id: Report folder ID to filter by (required if channel_id not provided)
            shift_id: Optional shift ID to filter by
        
        Returns:
            Dictionary containing:
            - 'content': CSV content as string
            - 'filename': Suggested filename for the CSV
        """
        # Get analyses
        analyses = CSVExportService.get_transcription_analyses_for_export(
            start_dt=start_dt,
            end_dt=end_dt,
            channel_id=channel_id,
            report_folder_id=report_folder_id,
            shift_id=shift_id
        )
        
        # Generate CSV content
        csv_content = CSVExportService.generate_csv_content(analyses)
        
        # Get channel_id for filename (either from parameter or from report_folder)
        filename_channel_id = channel_id
        if report_folder_id is not None and filename_channel_id is None:
            from data_analysis.models import ReportFolder
            try:
                report_folder = ReportFolder.objects.select_related('channel').get(id=report_folder_id)
                filename_channel_id = report_folder.channel.id
            except ReportFolder.DoesNotExist:
                filename_channel_id = None
        
        # Generate filename
        filename = CSVExportService.generate_csv_filename(
            channel_id=filename_channel_id,
            start_dt=start_dt,
            end_dt=end_dt,
            report_folder_id=report_folder_id,
            shift_id=shift_id
        )
        
        return {
            'content': csv_content,
            'filename': filename
        }
