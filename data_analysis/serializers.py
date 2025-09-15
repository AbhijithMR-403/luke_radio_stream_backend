from rest_framework import serializers
from data_analysis.models import AudioSegments as AudioSegmentsModel, TranscriptionDetail, TranscriptionAnalysis
from acr_admin.models import Channel


class TranscriptionAnalysisSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    summary = serializers.CharField(allow_null=True)
    sentiment = serializers.CharField(allow_null=True)
    general_topics = serializers.CharField(allow_null=True)
    iab_topics = serializers.CharField(allow_null=True)
    bucket_prompt = serializers.CharField(allow_null=True)
    created_at = serializers.DateTimeField(allow_null=True)


class TranscriptionDetailSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    transcript = serializers.CharField(allow_null=True)
    created_at = serializers.DateTimeField(allow_null=True)
    rev_job_id = serializers.CharField(allow_null=True)


class AudioSegmentSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    start_time = serializers.DateTimeField()
    end_time = serializers.DateTimeField()
    duration_seconds = serializers.FloatField()
    is_recognized = serializers.BooleanField()
    is_active = serializers.BooleanField()
    file_name = serializers.CharField(allow_null=True)
    file_path = serializers.CharField(allow_null=True)
    title = serializers.CharField(allow_null=True)
    title_before = serializers.CharField(allow_null=True)
    title_after = serializers.CharField(allow_null=True)
    notes = serializers.CharField(allow_null=True)
    created_at = serializers.DateTimeField(allow_null=True)
    is_analysis_completed = serializers.BooleanField()
    is_audio_downloaded = serializers.BooleanField()
    metadata_json = serializers.JSONField(allow_null=True)
    transcription = TranscriptionDetailSerializer(allow_null=True)
    analysis = TranscriptionAnalysisSerializer(allow_null=True)


class AudioSegmentsResponseSerializer(serializers.Serializer):
    segments = AudioSegmentSerializer(many=True)
    total_segments = serializers.IntegerField()
    total_recognized = serializers.IntegerField()
    total_unrecognized = serializers.IntegerField()
    total_with_transcription = serializers.IntegerField()
    total_with_analysis = serializers.IntegerField()


class ChannelInfoSerializer(serializers.Serializer):
    channel_id = serializers.CharField()
    project_id = serializers.CharField()
    channel_name = serializers.CharField()


class AudioSegmentsApiResponseSerializer(serializers.Serializer):
    success = serializers.BooleanField()
    data = AudioSegmentsResponseSerializer()
    channel_info = ChannelInfoSerializer()


class AudioSegmentsSerializer:
    """
    Custom serializer class to handle the complex data transformation
    for the AudioSegments API while maintaining the exact same response format.
    """
    
    @staticmethod
    def serialize_segments_data(db_segments):
        """
        Convert database segments to the expected response format.
        Uses prefetched data to avoid N+1 queries.
        """
        all_segments = []
        
        for segment in db_segments:
            segment_data = {
                'id': segment.id,
                'start_time': segment.start_time,
                'end_time': segment.end_time,
                'duration_seconds': segment.duration_seconds,
                'is_recognized': segment.is_recognized,
                'is_active': segment.is_active,
                'file_name': segment.file_name,
                'file_path': segment.file_path,
                'title': segment.title,
                'title_before': segment.title_before,
                'title_after': segment.title_after,
                'notes': segment.notes,
                'created_at': segment.created_at.isoformat() if segment.created_at else None,
                'is_analysis_completed': segment.is_analysis_completed,
                'is_audio_downloaded': segment.is_audio_downloaded,
                'metadata_json': segment.metadata_json
            }
            
            # Use prefetched transcription detail data (no database query needed)
            try:
                transcription_detail = segment.transcription_detail
                segment_data['transcription'] = {
                    'id': transcription_detail.id,
                    'transcript': transcription_detail.transcript,
                    'created_at': transcription_detail.created_at.isoformat() if transcription_detail.created_at else None,
                    'rev_job_id': transcription_detail.rev_job.job_id if transcription_detail.rev_job else None
                }
                
                # Use prefetched analysis data (no database query needed)
                try:
                    analysis = transcription_detail.analysis
                    segment_data['analysis'] = {
                        'id': analysis.id,
                        'summary': analysis.summary,
                        'sentiment': analysis.sentiment,
                        'general_topics': analysis.general_topics,
                        'iab_topics': analysis.iab_topics,
                        'bucket_prompt': analysis.bucket_prompt,
                        'created_at': analysis.created_at.isoformat() if analysis.created_at else None
                    }
                except AttributeError:
                    # No analysis found (prefetched data doesn't have analysis)
                    segment_data['analysis'] = None
                    
            except AttributeError:
                # No transcription detail found (prefetched data doesn't have transcription_detail)
                segment_data['transcription'] = None
                segment_data['analysis'] = None
            
            all_segments.append(segment_data)
        
        return all_segments
    
    @staticmethod
    def calculate_statistics(all_segments):
        """
        Calculate statistics for the segments data.
        Maintains the exact same logic as the original view.
        """
        # Count recognized and unrecognized segments
        total_recognized = sum(1 for segment in all_segments if segment["is_recognized"])
        total_unrecognized = sum(1 for segment in all_segments if not segment["is_recognized"])
        
        # Count segments with transcription and analysis
        total_with_transcription = sum(1 for segment in all_segments if segment.get("transcription") is not None)
        total_with_analysis = sum(1 for segment in all_segments if segment.get("analysis") is not None)
        
        return {
            "total_recognized": total_recognized,
            "total_unrecognized": total_unrecognized,
            "total_with_transcription": total_with_transcription,
            "total_with_analysis": total_with_analysis
        }
    
    @staticmethod
    def build_response(all_segments, channel):
        """
        Build the complete API response.
        Maintains the exact same structure as the original view.
        """
        statistics = AudioSegmentsSerializer.calculate_statistics(all_segments)
        
        result = {
            "segments": all_segments,
            "total_segments": len(all_segments),
            **statistics
        }
        
        return {
            'success': True,
            'data': result,
            'channel_info': {
                'channel_id': channel.channel_id,
                'project_id': channel.project_id,
                'channel_name': channel.name
            }
        }
