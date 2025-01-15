import cv2
import numpy as np
import os

def process_images(input_folder: str, output_folder: str):
    """
    Reads images from a folder, removes watermarks, reduces noise, enhances sharpness,
    and saves the processed images to another folder.

    Parameters:
    - input_folder: str
        Path to the folder containing input images.
    - output_folder: str
        Path to the folder where processed images will be saved.

    Returns:
    - None
    """
    try:
        # Ensure output folder exists
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Iterate through all files in the input folder
        for filename in os.listdir(input_folder):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, filename)

            # Skip directories
            if not os.path.isfile(input_path):
                print(f"Skipping directory: {filename}")
                continue

            # Check if the file is an image
            if not (filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.tiff'))):
                print(f"Skipping non-image file: {filename}")
                continue

            # Step 1: Open the image using OpenCV
            image = cv2.imread(input_path, cv2.IMREAD_UNCHANGED)

            if image is None:
                print(f"Error reading image: {input_path}. Skipping.")
                continue

            # Step 2: Remove watermark (replace transparency with white, if applicable)
            if len(image.shape) == 3 and image.shape[2] == 4:  # Check for alpha channel (transparency)
                alpha_channel = image[:, :, 3]  # Extract the alpha channel
                white_background = np.ones_like(image[:, :, :3], dtype=np.uint8) * 255
                alpha_factor = alpha_channel[:, :, None] / 255.0  # Normalize alpha channel
                image = (image[:, :, :3] * alpha_factor + white_background * (1 - alpha_factor)).astype(np.uint8)

            # Step 3: Convert to RGB if the image is not in RGB or RGBA
            if len(image.shape) == 2 or (len(image.shape) == 3 and image.shape[2] != 3):
                image = cv2.cvtColor(image, cv2.COLOR_GRAY2RGB)

            # Step 4: Reduce noise using Non-Local Means Denoising
            denoised_image = cv2.fastNlMeansDenoisingColored(image, None, 2, 10, 7, 21)

            # Step 5: Enhance sharpness using an unsharp mask
            gaussian = cv2.GaussianBlur(denoised_image, (9, 9), 10.0)
            sharpened_image = cv2.addWeighted(denoised_image, 1.5, gaussian, -0.5, 0)

            # Step 6: Save the processed image to the output folder
            cv2.imwrite(output_path, sharpened_image)
            print(f"Processed and saved: {output_path}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Example usage:
input_folder = r"C:\Image"  # Replace with your input folder path
output_folder = r"C:\Images"  # Replace with your output folder path

process_images(input_folder, output_folder)