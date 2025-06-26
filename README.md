# CGM Pattern Analysis for Food Diary Events
This repository provides code for analyzing continuous glucose monitoring (CGM) data in food diary entries from the [Shanghai T2D dataset](https://figshare.com/articles/dataset/Diabetes_Datasets-ShanghaiT1DM_and_ShanghaiT2DM/20444397?file=38259264) (Zhao et al., 2023), with a focus on clinical stratification and dietary pattern insights.

The analyses obtained with the jupyter notebook `main_analyses.ipynb` are described in the submission M5 (WP3) - EXplainable ML Model development, Sections 4 and 5.

## Overview
The code integrates:

- CGM response analysis: Aligns CGM data around food events and stratifies responses by biomarker values, caloric density, and meal type.

- Statistical comparisons: Quantifies significance across time, both between and within groups (e.g., high vs. low BMI or caloric intake).

- Data visualization: Generates interpretable plots of group differences and CGM trajectories.

- Raw diary preprocessing: Uses Pydantic models and LLMs (OpenAI / Anthropic) to transform free-text meal entries into structured nutritional data.

- Caching and parallelization: Supports efficient batch inference using local or cloud-based LLM APIs with built-in rate limiting.

## Key Features

- LLM-powered parsing of real-world food diaries with validation and error flagging

- Time-aligned CGM visualizations across meals and clinical strata

- Significance heatmaps and p-value overlays to highlight meaningful effects

- Local caching + structured outputs to streamline iterative workflows

- Flexible backends for OpenAI, Anthropic, vLLM, and LiteLLM-compatible endpoints



## Installation

Follow the steps below to run the project.

1. **Create and activate a virtual environment**:
   ```bash
   cd path/to/your/project
   python -m venv venv
   source venv/bin/activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Download the dataset**
Right now, the folder `./data` contains the file `processed_food_diary_entries.parquet`, which is the categorization of food events obtained with the LLM-pipeline. To run all analyses, you also need to store the Shanghai T2DM dataset there. Do so with this script:

```bash
python ./scripts/data_download.py
```


## How to use
 
Run `main_analyses.ipynb` to explore CGM responses and visualizations. 

If you want to obtain new classification results from an Anthropic or OpenAI LLM,

- rename the file `.env.copy` to `.env` in the root of the repository and add your API keys to the file.  

```bash
OPENAI_API_KEY=your_openai_api_key
ANTHROPIC_API_KEY=your_anthropic_api_key
```

- run `LLM_pipeline.ipynb`.


## Contact Information

For questions, please contact enrica.troiano@hk3lab.ai or tf@hk3lab.ai.