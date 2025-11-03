import json
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse

from acr_admin.models import Channel
from data_analysis.models import AudioSegments as AudioSegmentsModel, ReportFolder, SavedAudioSegment, AudioSegmentInsight


@method_decorator(csrf_exempt, name='dispatch')
class ReportFolderManagementView(View):
    """API to manage report folders (create, list, update, delete)"""
    
    def get(self, request, *args, **kwargs):
        """Get all report folders with their saved segments count"""
        try:
            folders = ReportFolder.objects.select_related('channel').prefetch_related('saved_segments').all()
            
            folders_data = []
            for folder in folders:
                folders_data.append({
                    'id': folder.id,
                    'name': folder.name,
                    'description': folder.description,
                    'color': folder.color,
                    'is_public': folder.is_public,
                    'channel': {
                        'id': folder.channel.id,
                        'name': folder.channel.name,
                        'channel_id': folder.channel.channel_id,
                        'project_id': folder.channel.project_id,
                    },
                    'saved_segments_count': folder.saved_segments.count(),
                    'created_at': folder.created_at.isoformat(),
                    'updated_at': folder.updated_at.isoformat()
                })
            
            return JsonResponse({
                'success': True,
                'data': {
                    'folders': folders_data,
                    'total_count': len(folders_data)
                }
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def post(self, request, *args, **kwargs):
        """Create a new report folder"""
        try:
            data = json.loads(request.body)
            
            name = data.get('name')
            description = data.get('description', '')
            color = data.get('color', '#3B82F6')
            is_public = data.get('is_public', True)
            channel_id = data.get('channel_id')
            
            if not name:
                return JsonResponse({'success': False, 'error': 'name is required'}, status=400)
            if not channel_id:
                return JsonResponse({'success': False, 'error': 'channel_id is required'}, status=400)
            try:
                channel = Channel.objects.get(id=channel_id, is_deleted=False)
            except Channel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
            
            # Validate color format
            if not color.startswith('#') or len(color) != 7:
                return JsonResponse({'success': False, 'error': 'color must be a valid hex color (e.g., #3B82F6)'}, status=400)
            
            folder = ReportFolder.objects.create(
                channel=channel,
                name=name,
                description=description,
                color=color,
                is_public=is_public
            )
            
            return JsonResponse({
                'success': True,
                'message': 'Report folder created successfully',
                'data': {
                    'id': folder.id,
                    'name': folder.name,
                    'description': folder.description,
                    'color': folder.color,
                    'is_public': folder.is_public,
                    'channel': {
                        'id': folder.channel.id,
                        'name': folder.channel.name,
                        'channel_id': folder.channel.channel_id,
                        'project_id': folder.channel.project_id,
                    },
                    'saved_segments_count': 0,
                    'created_at': folder.created_at.isoformat(),
                    'updated_at': folder.updated_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def put(self, request, folder_id, *args, **kwargs):
        """Update an existing report folder"""
        try:
            data = json.loads(request.body)
            
            try:
                folder = ReportFolder.objects.get(id=folder_id)
            except ReportFolder.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Report folder not found'}, status=404)
            
            # Update fields if provided
            if 'name' in data:
                folder.name = data['name']
            if 'description' in data:
                folder.description = data['description']
            if 'color' in data:
                color = data['color']
                if not color.startswith('#') or len(color) != 7:
                    return JsonResponse({'success': False, 'error': 'color must be a valid hex color (e.g., #3B82F6)'}, status=400)
                folder.color = color
            if 'is_public' in data:
                folder.is_public = data['is_public']
            if 'channel_id' in data:
                try:
                    channel = Channel.objects.get(id=data['channel_id'], is_deleted=False)
                except Channel.DoesNotExist:
                    return JsonResponse({'success': False, 'error': 'Channel not found'}, status=404)
                folder.channel = channel
            
            folder.save()
            
            return JsonResponse({
                'success': True,
                'message': 'Report folder updated successfully',
                'data': {
                    'id': folder.id,
                    'name': folder.name,
                    'description': folder.description,
                    'color': folder.color,
                    'is_public': folder.is_public,
                    'channel': {
                        'id': folder.channel.id,
                        'name': folder.channel.name,
                        'channel_id': folder.channel.channel_id,
                        'project_id': folder.channel.project_id,
                    },
                    'saved_segments_count': folder.saved_segments.count(),
                    'created_at': folder.created_at.isoformat(),
                    'updated_at': folder.updated_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def delete(self, request, folder_id, *args, **kwargs):
        """Delete a report folder and all its saved segments"""
        try:
            try:
                folder = ReportFolder.objects.get(id=folder_id)
            except ReportFolder.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Report folder not found'}, status=404)
            
            folder_name = folder.name
            folder.delete()
            
            return JsonResponse({
                'success': True,
                'message': f'Report folder "{folder_name}" deleted successfully'
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class SaveAudioSegmentView(View):
    """API to save audio segments to report folders with insights"""
    
    def post(self, request, *args, **kwargs):
        """Save an audio segment to a report folder"""
        try:
            data = json.loads(request.body)
            
            folder_id = data.get('folder_id')
            audio_segment_id = data.get('audio_segment_id')
            is_favorite = data.get('is_favorite', False)
            
            if not folder_id:
                return JsonResponse({'success': False, 'error': 'folder_id is required'}, status=400)
            
            if not audio_segment_id:
                return JsonResponse({'success': False, 'error': 'audio_segment_id is required'}, status=400)
            
            # Validate folder exists
            try:
                folder = ReportFolder.objects.get(id=folder_id)
            except ReportFolder.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Report folder not found'}, status=404)
            
            # Validate audio segment exists
            try:
                audio_segment = AudioSegmentsModel.objects.get(id=audio_segment_id)
            except AudioSegmentsModel.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Audio segment not found'}, status=404)
            
            # Check if already saved in this folder
            existing_save = SavedAudioSegment.objects.filter(
                folder=folder, 
                audio_segment=audio_segment
            ).first()
            
            if existing_save:
                return JsonResponse({
                    'success': False, 
                    'error': 'Audio segment is already saved in this folder'
                }, status=400)
            
            # Create the saved audio segment
            saved_segment = SavedAudioSegment.objects.create(
                folder=folder,
                audio_segment=audio_segment,
                is_favorite=is_favorite
            )
            
            return JsonResponse({
                'success': True,
                'message': 'Audio segment saved to folder successfully',
                'data': {
                    'id': saved_segment.id,
                    'folder_id': folder.id,
                    'folder_name': folder.name,
                    'audio_segment_id': audio_segment.id,
                    'audio_segment_title': audio_segment.title or 'Untitled',
                    'is_favorite': is_favorite,
                    'saved_at': saved_segment.saved_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def delete(self, request, saved_segment_id, *args, **kwargs):
        """Remove an audio segment from a folder"""
        try:
            try:
                saved_segment = SavedAudioSegment.objects.get(id=saved_segment_id)
            except SavedAudioSegment.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Saved audio segment not found'}, status=404)
            
            folder_name = saved_segment.folder.name
            audio_segment_title = saved_segment.audio_segment.title or 'Untitled'
            saved_segment.delete()
            
            return JsonResponse({
                'success': True,
                'message': f'"{audio_segment_title}" removed from folder "{folder_name}"'
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class FolderContentsView(View):
    """API to retrieve saved audio segments from a specific folder"""
    
    def get(self, request, folder_id, *args, **kwargs):
        """Get all saved audio segments in a folder with full details"""
        try:
            try:
                folder = ReportFolder.objects.get(id=folder_id)
            except ReportFolder.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Report folder not found'}, status=404)
            
            # Get saved segments with related data
            saved_segments = SavedAudioSegment.objects.filter(
                folder=folder
            ).select_related(
                'audio_segment__channel'
            ).prefetch_related(
                'audio_segment__transcription_detail__rev_job',
                'audio_segment__transcription_detail__analysis',
                'insights'
            ).order_by('-saved_at')
            
            segments_data = []
            for saved_segment in saved_segments:
                audio_segment = saved_segment.audio_segment
                
                # Build segment data with transcription and analysis
                segment_data = {
                    'saved_segment_id': saved_segment.id,
                    'audio_segment_id': audio_segment.id,
                    'start_time': audio_segment.start_time.isoformat(),
                    'end_time': audio_segment.end_time.isoformat(),
                    'duration_seconds': audio_segment.duration_seconds,
                    'is_recognized': audio_segment.is_recognized,
                    'title': audio_segment.title,
                    'title_before': audio_segment.title_before,
                    'title_after': audio_segment.title_after,
                    'file_name': audio_segment.file_name,
                    'file_path': audio_segment.file_path,
                    'channel_name': audio_segment.channel.name,
                    'channel_id': audio_segment.channel.channel_id,
                    'is_favorite': saved_segment.is_favorite,
                    'saved_at': saved_segment.saved_at.isoformat(),
                    'updated_at': saved_segment.updated_at.isoformat()
                }
                
                # Add transcription data if available
                try:
                    transcription_detail = audio_segment.transcription_detail
                    segment_data['transcription'] = {
                        'id': transcription_detail.id,
                        'transcript': transcription_detail.transcript,
                        'created_at': transcription_detail.created_at.isoformat(),
                        'rev_job_id': transcription_detail.rev_job.job_id if transcription_detail.rev_job else None
                    }
                    
                    # Add analysis data if available
                    try:
                        analysis = transcription_detail.analysis
                        segment_data['analysis'] = {
                            'id': analysis.id,
                            'summary': analysis.summary,
                            'sentiment': analysis.sentiment,
                            'general_topics': analysis.general_topics,
                            'iab_topics': analysis.iab_topics,
                            'bucket_prompt': analysis.bucket_prompt,
                            'created_at': analysis.created_at.isoformat()
                        }
                    except AttributeError:
                        segment_data['analysis'] = None
                        
                except AttributeError:
                    segment_data['transcription'] = None
                    segment_data['analysis'] = None
                
                # Add insights data
                insights_data = []
                for insight in saved_segment.insights.all():
                    insights_data.append({
                        'id': insight.id,
                        'title': insight.title,
                        'description': insight.description,
                        'created_at': insight.created_at.isoformat(),
                        'updated_at': insight.updated_at.isoformat()
                    })
                segment_data['insights'] = insights_data
                
                segments_data.append(segment_data)
            
            return JsonResponse({
                'success': True,
                'data': {
                    'folder': {
                        'id': folder.id,
                        'name': folder.name,
                        'description': folder.description,
                        'color': folder.color,
                        'is_public': folder.is_public,
                        'created_at': folder.created_at.isoformat(),
                        'channel': {
                            'id': folder.channel.id,
                            'name': folder.channel.name,
                            'channel_id': folder.channel.channel_id,
                            'project_id': folder.channel.project_id,
                        }
                    },
                    'saved_segments': segments_data,
                    'total_count': len(segments_data)
                }
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)


@method_decorator(csrf_exempt, name='dispatch')
class AudioSegmentInsightsView(View):
    """API to manage insights for saved audio segments"""
    
    def get(self, request, saved_segment_id, *args, **kwargs):
        """Get all insights for a saved audio segment"""
        try:
            try:
                saved_segment = SavedAudioSegment.objects.get(id=saved_segment_id)
            except SavedAudioSegment.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Saved audio segment not found'}, status=404)
            
            insights = AudioSegmentInsight.objects.filter(
                saved_audio_segment=saved_segment
            ).order_by('-created_at')
            
            insights_data = []
            for insight in insights:
                insights_data.append({
                    'id': insight.id,
                    'title': insight.title,
                    'description': insight.description,
                    'created_at': insight.created_at.isoformat(),
                    'updated_at': insight.updated_at.isoformat()
                })
            
            return JsonResponse({
                'success': True,
                'data': {
                    'saved_segment_id': saved_segment.id,
                    'audio_segment_title': saved_segment.audio_segment.title or 'Untitled',
                    'folder_name': saved_segment.folder.name,
                    'insights': insights_data,
                    'total_count': len(insights_data)
                }
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def post(self, request, saved_segment_id, *args, **kwargs):
        """Create a new insight for a saved audio segment"""
        try:
            data = json.loads(request.body)
            
            title = data.get('title')
            description = data.get('description')
            
            if not title:
                return JsonResponse({'success': False, 'error': 'title is required'}, status=400)
            
            if not description:
                return JsonResponse({'success': False, 'error': 'description is required'}, status=400)
            
            try:
                saved_segment = SavedAudioSegment.objects.get(id=saved_segment_id)
            except SavedAudioSegment.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Saved audio segment not found'}, status=404)
            
            insight = AudioSegmentInsight.objects.create(
                saved_audio_segment=saved_segment,
                title=title,
                description=description
            )
            
            return JsonResponse({
                'success': True,
                'message': 'Insight created successfully',
                'data': {
                    'id': insight.id,
                    'title': insight.title,
                    'description': insight.description,
                    'saved_segment_id': saved_segment.id,
                    'audio_segment_title': saved_segment.audio_segment.title or 'Untitled',
                    'folder_name': saved_segment.folder.name,
                    'created_at': insight.created_at.isoformat(),
                    'updated_at': insight.updated_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def put(self, request, saved_segment_id, insight_id, *args, **kwargs):
        """Update an existing insight"""
        try:
            data = json.loads(request.body)
            
            try:
                insight = AudioSegmentInsight.objects.get(
                    id=insight_id,
                    saved_audio_segment_id=saved_segment_id
                )
            except AudioSegmentInsight.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Insight not found'}, status=404)
            
            # Update fields if provided
            if 'title' in data:
                insight.title = data['title']
            if 'description' in data:
                insight.description = data['description']
            
            insight.save()
            
            return JsonResponse({
                'success': True,
                'message': 'Insight updated successfully',
                'data': {
                    'id': insight.id,
                    'title': insight.title,
                    'description': insight.description,
                    'saved_segment_id': insight.saved_audio_segment.id,
                    'audio_segment_title': insight.saved_audio_segment.audio_segment.title or 'Untitled',
                    'folder_name': insight.saved_audio_segment.folder.name,
                    'created_at': insight.created_at.isoformat(),
                    'updated_at': insight.updated_at.isoformat()
                }
            })
            
        except json.JSONDecodeError:
            return JsonResponse({'success': False, 'error': 'Invalid JSON data'}, status=400)
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
    def delete(self, request, saved_segment_id, insight_id, *args, **kwargs):
        """Delete an insight"""
        try:
            try:
                insight = AudioSegmentInsight.objects.get(
                    id=insight_id,
                    saved_audio_segment_id=saved_segment_id
                )
            except AudioSegmentInsight.DoesNotExist:
                return JsonResponse({'success': False, 'error': 'Insight not found'}, status=404)
            
            insight_title = insight.title
            audio_segment_title = insight.saved_audio_segment.audio_segment.title or 'Untitled'
            insight.delete()
            
            return JsonResponse({
                'success': True,
                'message': f'Insight "{insight_title}" deleted from "{audio_segment_title}"'
            })
            
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
