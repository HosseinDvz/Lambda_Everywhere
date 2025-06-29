import pandas as pd
import re
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer
from tqdm import tqdm

class WebsiteClassifier:
    """
    A zero-shot text classifier for website content using Hugging Face's
    facebook/bart-large-mnli model. It can classify single rows or entire
    CSV files into predefined categories such as news, education, or shopping.
    """

    def __init__(self, labels=None):
        """
        Initializes the classifier with default or user-provided labels.
        
        Args:
            labels (list of str, optional): Categories to classify content into.
                                             Defaults to a predefined set.
        """
        self.labels = labels or [
            "educational", "news", "entertainment", "shopping",
            "social media", "health", "finance", "sports"
        ]
        
        # === Model Path Configuration ===
        # Use the following path for local development (relative to project root)
        MODEL_PATH = "./model/models--facebook--bart-large-mnli/snapshots/d7645e127eaf1aefc7862fd59a17a5aa8558b8ce"
        #model/models--facebook--bart-large-mnli/snapshots/d7645e127eaf1aefc7862fd59a17a5aa8558b8ce

        # Use this absolute path when running inside the Docker container (e.g., AWS deployment)
        # MODEL_PATH = "/app/model/models--facebook--bart-large-mnli/snapshots/d7645e127eaf1aefc7862fd59a17a5aa8558b8ce"

        self.classifier = pipeline(
            "zero-shot-classification",
            model=MODEL_PATH,
            tokenizer=MODEL_PATH,
            device=-1 # keep using cpu if starting the app with gunicorn
        )

        # Uncomment the following if you cloned the notebook and do not have the model in the above folder
        # self.classifier = pipeline( "zero-shot-classification", model="facebook/bart-large-mnli", device=-1)  # Use CPU while using gunicorn

    def clean_text(self, text: str) -> str:
        """
        Cleans input text by removing common HTTP error codes and collapsing whitespace.
        
        Args:
            text (str): Raw input text (website content or URL).
        
        Returns:
            str: Cleaned text suitable for model input.
        """
        if not isinstance(text, str):
            return ""
        text = re.sub(r"\b(403|404|500|302)\b.*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def classify_row(self, website: str, content: str) -> str:
        """
        Classifies a single website + content pair using zero-shot classification.
        
        Args:
            website (str): The domain or URL of the website.
            content (str): The text content scraped from the website.
        
        Returns:
            str: Predicted label from the list of predefined categories.
        """
        clean_site = self.clean_text(website)
        clean_text = self.clean_text(content)
        combined = f"{clean_site}: {clean_text}"
        if not clean_text:
            return "unknown"
        result = self.classifier(combined[:1000], self.labels) 
        return result["labels"][0]

    def add_label_column(self, input_csv: str, output_csv: str):
        """
        Reads a CSV, classifies each row, and adds a 'predicted_label' column.
        
        Args:
            input_csv (str): Path to the input CSV with 'website' and 'content' columns.
            output_csv (str): Path to save the labeled output CSV.
        
        Returns:
            None
        """
        df = pd.read_csv(input_csv)
        tqdm.pandas(desc="Classifying")
        df["predicted_label"] = df.progress_apply(
            lambda row: self.classify_row(row["website"], row["content"]), axis=1
        )
        df.to_csv(output_csv, index=False)
        print(f"✅ Output saved to {output_csv}")


if __name__ == "__main__":
    input_path = "scrapingResults/chunk_1_labeled.csv"
    output_path = "chunk_1_labeled.csv"
    clf = WebsiteClassifier()
    clf.add_label_column(input_path, output_path)
