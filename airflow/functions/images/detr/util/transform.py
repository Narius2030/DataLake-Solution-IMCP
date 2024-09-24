from io import BytesIO
import torchvision.transforms as T
from PIL import Image
import string

# Reshape image
def reshape(image_response):
    transform = T.Compose([
        T.Resize((240, 240)),
        T.ToTensor(),
        T.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    ])
    image = Image.open(BytesIO(image_response))
    if image.mode != "RGB":
        image = image.convert("RGB")
    return transform(image).unsqueeze(0)

# To remove punctuations
def remove_punctuation(text_original):
    text_no_punctuation = text_original.translate(string.punctuation)
    return(text_no_punctuation)

# To remove single characters
def remove_single_character(text):
    text_len_more_than1 = ""
    for word in text.split():
        if len(word) > 1:
            text_len_more_than1 += " " + word
    return(text_len_more_than1)

# To remove numeric values
def remove_numeric(text):
    text_no_numeric = ""
    for word in text.split():
        isalpha = word.isalpha()
        if isalpha:
            text_no_numeric += " " + word
    return(text_no_numeric)

def assign_tokens(captions):
    all_captions = []
    for caption in captions:
        caption = '<start> ' + caption+ ' <end>'
        all_captions.append(caption)
    return all_captions