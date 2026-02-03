import io
import os
import logging
import tempfile
from PIL import Image, ImageDraw, ImageFont
import fitz
try:
    from moviepy.editor import VideoFileClip, ImageClip, CompositeVideoClip
except ImportError:
    from moviepy import VideoFileClip, ImageClip, CompositeVideoClip
import random
import asyncio
import shutil

def _get_video_duration(input_path):
    """
    Get video duration in seconds using ffprobe.
    Returns float duration or None on failure.
    """
    import subprocess
    import shutil
    
    if not shutil.which('ffprobe'):
        logging.warning("ffprobe not found in system path")
        return None
        
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', input_path],
            capture_output=True,
            text=True,
            timeout=5
        )
        return float(result.stdout.strip())
    except Exception as e:
        logging.debug(f"ffprobe duration check failed: {e}")
        return None

def _generate_watermark_intervals(duration):
    """
    Generate time intervals for watermarking:
    - First 5 seconds
    - Last 5 seconds
    - Random 2-second bursts every ~30 seconds in between
    Returns list of tuples (start, end)
    """
    intervals = []
    
    # 1. Beginning (0-5s)
    intervals.append((0, min(5, duration)))
    
    # If video is very short, just return what we have (or full duration if < 5s)
    if duration <= 10:
        if duration > 5:
            intervals.append((duration - 5, duration))
        return intervals

    # 2. End (duration-5s to duration)
    intervals.append((duration - 5, duration))
    
    # 3. Random intervals in between
    # Available window: from 5s to (duration - 5s)
    start_window = 5
    end_window = duration - 5
    window_len = end_window - start_window
    
    if window_len > 10: # Only add intermediate if enough space
        # Add a burst roughly every 30 seconds
        num_bursts = int(window_len / 30)
        step = window_len / (num_bursts + 1)
        
        for i in range(1, num_bursts + 1):
            base_time = start_window + (i * step)
            # Add some randomness +/- 5s, but ensure 2s clip fits
            drift = random.uniform(-5, 5)
            burst_start = max(start_window, min(end_window - 2, base_time + drift))
            intervals.append((burst_start, burst_start + 2))
            
    return sorted(intervals)

def _detect_ffmpeg_encoder():
    """
    Detect available FFmpeg hardware encoder in priority order.
    Verifies the encoder actually works by performing a small test encoding.
    Returns tuple: (encoder_name, is_hardware_accelerated)
    """
    import subprocess
    import shutil
    
    # Check if FFmpeg is available
    if not shutil.which('ffmpeg'):
        logging.warning("ffmpeg not found in system path")
        return None, False
    
    # Hardware encoders in priority order (fastest first)
    hw_encoders = [
        'h264_nvenc',   # NVIDIA
        'h264_amf',     # AMD
        'h264_qsv',     # Intel
    ]
    
    def test_encoder(enc_name):
        """Try to encode 1 second of black video with the encoder"""
        try:
            # Generate 1s of black video and encode to null output
            cmd = [
                'ffmpeg', '-y', 
                '-f', 'lavfi', '-i', 'color=size=64x64:duration=1', 
                '-c:v', enc_name, 
                '-f', 'null', '-'
            ]
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True,
                timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False

    # Check hardware encoders
    for encoder in hw_encoders:
        if test_encoder(encoder):
            logging.info(f"Hardware encoder confirmed working: {encoder}")
            return encoder, True
        else:
            logging.debug(f"Hardware encoder {encoder} present but failed runtime test")
    
    # Check software fallback
    if test_encoder('libx264'):
        # logging.info("Using software encoder: libx264")
        return 'libx264', False
            
    logging.warning("No working video encoder found")
    return None, False

def _apply_watermark_ffmpeg(input_path, output_path, watermark_text, encoder, is_hw_accel):
    """
    Apply watermark using FFmpeg with drawtext filter and 'enable' expression for segmented display.
    """
    import subprocess
    
    # logging.info(f"Applying FFmpeg watermark with encoder={encoder}, hw_accel={is_hw_accel}")

    # Get duration to calculate intervals
    duration = _get_video_duration(input_path)
    if not duration:
        # If can't get duration, fallback to always on (better than nothing)
        logging.warning("Could not determine duration, falling back to full-video watermark")
        enable_expr = "1"
    else:
        intervals = _generate_watermark_intervals(duration)
        # Build enable expression: betweeen(t,start,end)+between(...)
        terms = [f"between(t,{start:.2f},{end:.2f})" for start, end in intervals]
        enable_expr = "+".join(terms)
        # logging.info(f"FFmpeg enable expression: {enable_expr}")

    # Escape special characters for FFmpeg drawtext
    escaped_text = watermark_text.replace("'", "\\'").replace(":", "\\:")
    
    # Build drawtext filter - position at bottom right with padding
    drawtext_filter = (
        f"drawtext=text='{escaped_text}':"
        f"fontsize=24:"
        f"fontcolor=white@0.8:"
        f"bordercolor=black@0.6:"
        f"borderw=1:"
        f"x=w-tw-20:"
        f"y=h-th-20:"
        f"enable='{enable_expr}'"
    )
    
    # Build FFmpeg command
    # NOTE: We use software decoding (default) + hardware encoding.
    # Mixing hw decoding (-hwaccel cuda) with cpu filters (drawtext) requires complex 
    # hwdownload/upload graphs, otherwise it fails.
    cmd = ['ffmpeg', '-y', '-i', input_path]
    
    # Add filter and encoding options
    cmd.extend([
        '-vf', drawtext_filter,
        '-c:v', encoder,
        '-c:a', 'aac',
        '-movflags', '+faststart'
    ])
    
    # Add encoder-specific presets
    if encoder == 'h264_nvenc':
        cmd.extend(['-preset', 'p1', '-tune', 'll'])  # Fastest NVIDIA preset
    elif encoder == 'h264_amf':
        cmd.extend(['-quality', 'speed'])
    elif encoder == 'h264_qsv':
        cmd.extend(['-preset', 'veryfast'])
    else:  # libx264
        cmd.extend(['-preset', 'ultrafast', '-crf', '23'])
    
    cmd.append(output_path)
    
    # logging.info(f"Running FFmpeg command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0 and os.path.exists(output_path):
            # logging.info(f"FFmpeg watermarking successful with {encoder} (HW: {is_hw_accel})")
            return True
        else:
            logging.warning(f"FFmpeg failed with return code {result.returncode}")
            if result.stderr:
                logging.warning(f"FFmpeg stderr: {result.stderr[-1000:]}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.warning("FFmpeg watermarking timed out")
        return False
    except Exception as e:
        logging.warning(f"FFmpeg error: {e}")
        return False

def _apply_watermark_moviepy(input_path, output_path, watermark_text, temp_dir):
    """
    Fallback: Apply watermark using MoviePy (segmented clips).
    """
    # logging.info("Starting MoviePy watermark fallback")
    video_clip = None
    watermark_clips = []
    final_clip = None
    watermark_img_path = os.path.join(temp_dir, "watermark.png")
    
    try:
        video_clip = VideoFileClip(input_path)
        duration = video_clip.duration
        # logging.info(f"MoviePy loaded video, duration: {duration}s")
        
        font_size = max(20, int(video_clip.w * 0.025))
        try:
            font = ImageFont.truetype("arial.ttf", font_size)
        except IOError:
            font = ImageFont.load_default()
        
        dummy_draw = ImageDraw.Draw(Image.new("RGBA", (1, 1)))
        if hasattr(dummy_draw, 'textbbox'):
            bbox = dummy_draw.textbbox((0, 0), watermark_text, font=font)
            text_width, text_height = bbox[2] - bbox[0], bbox[3] - bbox[1]
        else:
            text_width, text_height = dummy_draw.textsize(watermark_text, font=font)
        
        img_width = text_width + 20
        img_height = text_height + 20
        txt_img = Image.new('RGBA', (img_width, img_height), (255, 255, 255, 0))
        draw = ImageDraw.Draw(txt_img)
        draw.text((2, 2), watermark_text, font=font, fill=(0, 0, 0, 160))
        draw.text((0, 0), watermark_text, font=font, fill=(255, 255, 255, 200))
        txt_img.save(watermark_img_path, "PNG")
        
        # Generate clips for each interval
        intervals = _generate_watermark_intervals(duration)
        # logging.info(f"MoviePy applying watermark intervals: {intervals}")
        
        # Configure base watermark clip
        # Try MoviePy v2 API first (with_), fallback to v1 (set_)
        base_clip = ImageClip(watermark_img_path)
        
        try:
            # MoviePy v2
            base_wm_clip = base_clip.with_position(('right', 'bottom')).with_opacity(0.8)
            # Check for margin method which might be same name
            if hasattr(base_wm_clip, 'margin'):
                base_wm_clip = base_wm_clip.margin(right=20, bottom=20, opacity=0)
            # logging.info("Using MoviePy v2 API")
        except AttributeError:
            # MoviePy v1
            base_wm_clip = base_clip.set_position(('right', 'bottom')).set_opacity(0.8)
            base_wm_clip = base_wm_clip.margin(right=20, bottom=20, opacity=0)
            # logging.info("Using MoviePy v1 API")

        for start, end in intervals:
            try:
                # Ensure we don't exceed video duration
                if start >= duration: continue
                clip_duration = min(end, duration) - start
                if clip_duration <= 0: continue
                
                try:
                    # v2
                    wm_clip = base_wm_clip.with_start(start).with_duration(clip_duration)
                except AttributeError:
                    # v1
                    wm_clip = base_wm_clip.set_start(start).set_duration(clip_duration)
                    
                watermark_clips.append(wm_clip)
            except Exception as e:
                logging.warning(f"Failed to create watermark clip for interval {start}-{end}: {e}")

        final_clip = CompositeVideoClip([video_clip] + watermark_clips)
        final_clip.write_videofile(
            output_path,
            codec='libx264',
            audio_codec='aac',
            temp_audiofile=os.path.join(temp_dir, 'temp-audio.m4a'),
            remove_temp=True,
            verbose=False,
            logger=None,
            preset='ultrafast'
        )
        return True
    except Exception as e:
        logging.error(f"MoviePy watermarking failed: {e}", exc_info=True)
        return False
    finally:
        if final_clip: final_clip.close()
        for clip in watermark_clips:
            try: clip.close() 
            except: pass
        if video_clip: video_clip.close()

def apply_watermark_to_video(video_bytes, watermark_text, filename):
    """
    Apply watermark to video using FFmpeg (fast, with HW acceleration) or MoviePy (fallback).
    Watermark is applied only at start, end, and random intervals to save resources.
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Ensure output filename has .mp4 extension
            base_name = os.path.splitext(filename)[0]
            temp_video_path = os.path.join(temp_dir, filename)
            output_video_path = os.path.join(temp_dir, f"watermarked_{base_name}.mp4")
            
            with open(temp_video_path, 'wb') as f:
                f.write(video_bytes)
            
            # Try FFmpeg first (faster with potential hardware acceleration)
            encoder, is_hw_accel = _detect_ffmpeg_encoder()
            
            if encoder:
                # logging.info(f"Using FFmpeg with encoder: {encoder} (HW accelerated: {is_hw_accel})")
                success = _apply_watermark_ffmpeg(
                    temp_video_path, output_video_path, watermark_text, encoder, is_hw_accel
                )
                
                if success:
                    with open(output_video_path, 'rb') as f:
                        return f.read(), "video/mp4"
                else:
                    logging.warning("FFmpeg failed, falling back to MoviePy")
            # else:
            #     logging.info("FFmpeg not available, using MoviePy")
            
            # Fallback to MoviePy
            success = _apply_watermark_moviepy(temp_video_path, output_video_path, watermark_text, temp_dir)
            
            if success:
                with open(output_video_path, 'rb') as f:
                    return f.read(), "video/mp4"
            else:
                # Return original if all methods fail
                return video_bytes, "video/mp4"
                
    except Exception as e:
        logging.error(f"Error watermarking video: {e}", exc_info=True)
        return video_bytes, "video/mp4"

def apply_watermark_to_image(image_bytes, watermark_text):
    try:
        base_image = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
        width, height = base_image.size
        txt_layer = Image.new('RGBA', base_image.size, (255, 255, 255, 0))
        draw = ImageDraw.Draw(txt_layer)
        font_size = max(10, int(width * 0.015))
        try:
            font = ImageFont.truetype("arial.ttf", font_size)
        except IOError:
            font = ImageFont.load_default()

        if hasattr(draw, 'textbbox'):
            bbox = draw.textbbox((0, 0), watermark_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
        else:
            text_width, text_height = draw.textsize(watermark_text, font=font)

        padding_x = 20
        padding_y = 20
        x = width - text_width - padding_x
        y = height - text_height - padding_y
        draw.text((x + 1, y + 1), watermark_text, font=font, fill=(0, 0, 0, 160))
        draw.text((x, y), watermark_text, font=font, fill=(255, 255, 255, 200))
        watermarked = Image.alpha_composite(base_image, txt_layer)
        output_buffer = io.BytesIO()
        watermarked.convert("RGB").save(output_buffer, format="JPEG", quality=95)
        return output_buffer.getvalue(), "image/jpeg"
    except Exception as e:
        logging.error(f"Error watermarking image: {e}", exc_info=True)
        return image_bytes, "image/jpeg"

def apply_watermark_to_pdf(pdf_bytes, watermark_text):
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        for page in doc:
            rect = page.rect
            fontsize = 8
            text_width = fitz.get_text_length(watermark_text, fontname="helv", fontsize=fontsize)
            x = rect.width - text_width - 20
            y = rect.height - 20
            page.insert_text((x + 0.5, y + 0.5), watermark_text, fontsize=fontsize, fontname="helv", color=(0, 0, 0),
                             fill_opacity=0.5)
            page.insert_text((x, y), watermark_text, fontsize=fontsize, fontname="helv", color=(1, 1, 1),
                             fill_opacity=0.8)
        output_buffer = io.BytesIO()
        doc.save(output_buffer)
        return output_buffer.getvalue(), "application/pdf"
    except Exception as e:
        logging.error(f"Error watermarking PDF: {e}", exc_info=True)
        return pdf_bytes, "application/pdf"

async def _detect_ffmpeg_encoder_async():
    """
    Async version of _detect_ffmpeg_encoder.
    Detects available FFmpeg hardware encoder in priority order.
    Verifies the encoder actually works by performing a small test encoding.
    """
    import shutil
    
    # Check if FFmpeg is available
    if not shutil.which('ffmpeg'):
        logging.warning("ffmpeg not found in system path")
        return None, False
    
    # Hardware encoders in priority order (fastest first)
    hw_encoders = [
        'h264_qsv',     # Intel (Preferred if available)
        'h264_nvenc',   # NVIDIA
        'h264_amf',     # AMD
    ]
    
    async def test_encoder(enc_name):
        """Try to encode 1 second of black video with the encoder"""
        try:
            # Generate 1s of black video and encode to null output
            cmd = [
                'ffmpeg', '-y', 
                '-f', 'lavfi', '-i', 'color=size=64x64:duration=1', 
                '-c:v', enc_name, 
                '-f', 'null', '-'
            ]
            
            # Use asyncio subprocess
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
                return process.returncode == 0
            except asyncio.TimeoutError:
                try: process.kill()
                except: pass
                return False
                
        except Exception:
            return False

    # Check hardware encoders
    for encoder in hw_encoders:
        if await test_encoder(encoder):
            # logging.info(f"Hardware encoder confirmed working: {encoder}")
            return encoder, True
        else:
            logging.debug(f"Hardware encoder {encoder} present but failed runtime test")
    
    # Check software fallback
    if await test_encoder('libx264'):
        # logging.info("Using software encoder: libx264")
        return 'libx264', False
            
    logging.warning("No working video encoder found")
    return None, False

async def _apply_watermark_ffmpeg_async(input_path, output_path, watermark_text, encoder, is_hw_accel):
    """
    Async version of _apply_watermark_ffmpeg.
    Supports cancellation (killing process).
    """
    import subprocess
    
    # Escape special characters for FFmpeg drawtext
    escaped_text = watermark_text.replace("'", "\\'").replace(":", "\\:")
    
    # Get duration (using sync function is fine as checks are fast, or could update to async)
    loop = asyncio.get_running_loop()
    duration = await loop.run_in_executor(None, _get_video_duration, input_path)

    if not duration:
        logging.warning("Could not determine duration, falling back to full-video watermark")
        enable_expr = "1"
    else:
        intervals = _generate_watermark_intervals(duration)
        terms = [f"between(t,{start:.2f},{end:.2f})" for start, end in intervals]
        enable_expr = "+".join(terms)
        # logging.info(f"FFmpeg enable expression: {enable_expr}")

    # Build drawtext filter - position at bottom right with padding
    drawtext_filter = (
        f"drawtext=text='{escaped_text}':"
        f"fontsize=24:"
        f"fontcolor=white@0.8:"
        f"bordercolor=black@0.6:"
        f"borderw=1:"
        f"x=w-tw-20:"
        f"y=h-th-20:"
        f"enable='{enable_expr}'"
    )
    
    cmd = ['ffmpeg', '-y', '-i', input_path]
    cmd.extend([
        '-vf', drawtext_filter,
        '-c:v', encoder,
        '-c:a', 'aac',
        '-movflags', '+faststart'
    ])
    
    # Add encoder-specific presets
    if encoder == 'h264_qsv':
        cmd.extend(['-preset', 'veryfast'])
    elif encoder == 'h264_nvenc':
        cmd.extend(['-preset', 'p1', '-tune', 'll'])
    elif encoder == 'h264_amf':
        cmd.extend(['-quality', 'speed'])
    else:  # libx264
        cmd.extend(['-preset', 'ultrafast', '-crf', '23'])
    
    cmd.append(output_path)
    
    # logging.info(f"Running FFmpeg async command: {' '.join(cmd)}")
    
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Wait for completion with timeout
        try:
            # Wait for finish
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0 and os.path.exists(output_path):
                # logging.info(f"FFmpeg async watermarking successful with {encoder} (HW: {is_hw_accel})")
                return True
            else:
                logging.warning(f"FFmpeg async failed (RC={process.returncode})")
                if stderr:
                    logging.warning(f"FFmpeg stderr: {stderr.decode()[-1000:]}")
                return False
                
        except asyncio.CancelledError:
            logging.warning("FFmpeg watermarking task cancelled by user - killing process")
            try:
                process.kill()
                await process.wait() # Reap zombie
            except Exception as e:
                logging.error(f"Error killing process: {e}")
            raise # Propagate cancellation
            
    except Exception as e:
        logging.error(f"Async FFmpeg error: {e}")
        if process:
            try: process.kill() 
            except: pass
        return False

async def apply_watermark_to_video_async(video_bytes, watermark_text, filename):
    """
    Async version of apply_watermark_to_video. 
    Handles cancellation properly.
    """
    try:
        # We need to manage temp dir manually to ensure async cleanup
        loop = asyncio.get_running_loop()
        temp_dir = await loop.run_in_executor(None, tempfile.mkdtemp)
        
        try:
            base_name = os.path.splitext(filename)[0]
            temp_video_path = os.path.join(temp_dir, filename)
            output_video_path = os.path.join(temp_dir, f"watermarked_{base_name}.mp4")
            
            # Async write input file
            await loop.run_in_executor(None, lambda: open(temp_video_path, 'wb').write(video_bytes))
            
            # Detect encoder async
            encoder, is_hw_accel = await _detect_ffmpeg_encoder_async()
            
            success = False
            if encoder:
                 success = await _apply_watermark_ffmpeg_async(
                    temp_video_path, output_video_path, watermark_text, encoder, is_hw_accel
                )
            
            # Fallback to MoviePy (Sync wrap) if FFmpeg failed or not available
            if not success:
                # logging.info("Falling back to MoviePy (Sync wrapped) in async flow")
                # Wrap the sync moviepy function
                success = await loop.run_in_executor(None, _apply_watermark_moviepy, temp_video_path, output_video_path, watermark_text, temp_dir)
                
            if success:
                # Async read output
                with open(output_video_path, 'rb') as f:
                    data = await loop.run_in_executor(None, f.read)
                return data, "video/mp4"
            else:
                return video_bytes, "video/mp4"

        finally:
            # Cleanup temp dir
            await loop.run_in_executor(None, shutil.rmtree, temp_dir)
            
    except asyncio.CancelledError:
        logging.warning("Watermarking cancelled - cleaning up")
        # Ensure cleanup happens even on cancellation
        try: shutil.rmtree(temp_dir)
        except: pass
        raise
    except Exception as e:
        logging.error(f"Error watermarking video async: {e}", exc_info=True)
        return video_bytes, "video/mp4"