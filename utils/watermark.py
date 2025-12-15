import io
import os
import logging
import tempfile
from PIL import Image, ImageDraw, ImageFont
import fitz
from moviepy.editor import VideoFileClip, ImageClip, CompositeVideoClip


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


def apply_watermark_to_video(video_bytes, watermark_text, filename):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_video_path = os.path.join(temp_dir, filename)
            output_video_path = os.path.join(temp_dir, f"watermarked_{filename}")
            watermark_img_path = os.path.join(temp_dir, "watermark.png")
            with open(temp_video_path, 'wb') as f:
                f.write(video_bytes)
            video_clip = None
            watermark_clip = None
            final_clip = None
            try:
                video_clip = VideoFileClip(temp_video_path)
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
                watermark_clip = (
                    ImageClip(watermark_img_path)
                    .set_duration(video_clip.duration)
                    .set_position(('right', 'bottom'))
                    .set_opacity(0.8)
                    .margin(right=20, bottom=20, opacity=0)
                )
                final_clip = CompositeVideoClip([video_clip, watermark_clip])
                final_clip.write_videofile(
                    output_video_path,
                    codec='libx264',
                    audio_codec='aac',
                    temp_audiofile=os.path.join(temp_dir, 'temp-audio.m4a'),
                    remove_temp=True,
                    verbose=False,
                    logger=None,
                    preset='ultrafast'
                )
                with open(output_video_path, 'rb') as f:
                    return f.read(), "video/mp4"
            finally:
                if final_clip: final_clip.close()
                if watermark_clip: watermark_clip.close()
                if video_clip: video_clip.close()
    except Exception as e:
        logging.error(f"Error watermarking video: {e}", exc_info=True)
        return video_bytes, "video/mp4"