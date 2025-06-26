"""
The class ChineseCGMData will be used to process CGM and metadata from the Shanghai T2DM dataset, 
with tools to clean, align, aggregate, and extract event-based CGM segments with flexible temporal resolution.
"""


from typing import Literal, Optional
import polars as pl
import random
import os


class ChineseCGMData:
    def __init__(self,local_base_path: str):
        self.local_base_path = local_base_path
        self.metadata_file = os.path.join(local_base_path, "Shanghai_T2DM_Summary.xlsx")
        
        
        self.cgm_schema_polars = {'Date': pl.Datetime,
            'CGM': pl.Float64,
            'CBG': pl.Float64,
            'Blood Ketone': pl.Float64,
            'DI (Eng)': pl.String,
            'DI (Ch)': pl.String,
            'Insulin (s.c.)': pl.String,
            'NIHA': pl.String,
            'CSII Bolus': pl.Float64,
            'CSII Basal': pl.Float64,
            'Insulin (i.v.)': pl.String}
        self.transformation_rules_metadata = {"Age (years)": "age", "BMI (kg/m2)": "BMI"}
        self.transformation_rules_cgm = {
            "Date": "Date",
            "CGM ": "CGM",
            "CBG ": "CBG",
            "CGM (mg / dl)": "CGM",
            "CBG (mg / dl)": "CBG",
            "Blood Ketone (mmol / L)": "Blood Ketone",
            "Blood Ketone ": "Blood Ketone",
            "Dietary intake": "DI (Eng)",
            "饮食": "DI (Ch)",
            "Insulin dose - s.c.": "Insulin (s.c.)",
            "Non-insulin hypoglycemic agents": "NIHA",
            "CSII - bolus insulin (Novolin R, IU)": "CSII Bolus",
            "CSII - basal insulin (Novolin R, IU / H)": "CSII Basal",
            "Insulin dose - i.v.": "Insulin (i.v.)",
            "胰岛素泵基础量 (Novolin R, IU / H)": "CSII Basal",
            "进食量": "DI (Ch)",
            "CSII - bolus insulin ":"CSII Bolus",
            "CSII - bolus insulin (Novolin R  IU)": "CSII Bolus",
            "CSII - basal insulin": "CSII Basal",
            "CSII - bolus insulin": "CSII Bolus",
            "CSII - basal insulin (Novolin R  IU / H)": "CSII Basal",
            "CSII - basal insulin ":"CSII Basal",
        }
        self.df_metadata = self.prepare_metadata(pl.read_excel(self.metadata_file))
        self.cgm_df : pl.DataFrame = pl.concat(self.create_all_cgm_data(self.df_metadata["Patient Number"].to_list())).filter(pl.col("Date").is_not_null())
        self.metadata_columns = self.df_metadata.columns
        self.cgm_columns = self.cgm_df.columns
    
    def prepare_metadata(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Prepare metadata DataFrame by properly handling missing values and type casting.
        
        Args:
            df: Raw metadata DataFrame with string columns containing "/" for missing values
            
        Returns:
            Processed DataFrame with proper numeric types
        """
        # List of columns that should be numeric
        numeric_columns = [
            "Fasting Plasma Glucose (mg/dl)",
            "2-hour Postprandial Plasma Glucose (mg/dl)",
            "Fasting C-peptide (nmol/L)",
            "2-hour Postprandial C-peptide (nmol/L)",
            "Fasting Insulin (pmol/L)",
            "2-hour Postprandial insulin (pmol/L)",
            "HbA1c (mmol/mol)",
            "Glycated Albumin (%)",
            "Total Cholesterol (mmol/L)",
            "Triglyceride (mmol/L)",
            "High-Density Lipoprotein Cholesterol (mmol/L)",
            "Low-Density Lipoprotein Cholesterol (mmol/L)",
            "Creatinine (umol/L)",
            "Estimated Glomerular Filtration Rate (ml/min/1.73m2)",
            "Uric Acid (mmol/L)",
            "Blood Urea Nitrogen (mmol/L)",
            "Estimated Glomerular Filtration Rate  (ml/min/1.73m2) "
        ]
        
        # Create expressions to replace "/" with null and cast to float
        cast_expressions = [
            pl.col(col).str.replace("/", "").cast(pl.Float64, strict=False)
            for col in numeric_columns if col in df.columns
        ]
        
        # Get the column names that aren't in the numeric list
        other_columns = [col for col in df.columns if col not in numeric_columns]
        
        # Combine all columns in the final selection
        return df.select([
            *[pl.col(col) for col in other_columns],
            *cast_expressions
        ])

    def get_chinese_subject_cgm_file_path(self,subject_id: str) -> str:
        os_base_path = os.path.abspath(self.local_base_path)
        cgm_file_name =f"{subject_id}.xlsx"
        cgm_file_path = os.path.join(os_base_path, "Shanghai_T2DM", cgm_file_name)
        if not os.path.exists(cgm_file_path):
            cgm_file_path = cgm_file_path.replace(".xlsx",".xls")
            if not os.path.exists(cgm_file_path):
                raise FileNotFoundError(f"No CGM file found for subject {subject_id} at {cgm_file_path}")
        return cgm_file_path
    
    def load_single_subject_cgm_data(self,subject_id: str,daily:bool=False) -> pl.DataFrame:
        cgm_file_path = self.get_chinese_subject_cgm_file_path(subject_id)
        cgm_df = pl.read_excel(cgm_file_path)
        rename_dict = {key:value for key,value in self.transformation_rules_cgm.items() if key in cgm_df.columns}

        cgm_df = cgm_df.rename(rename_dict)
        cgm_df = cgm_df.with_columns(pl.lit(subject_id).alias("Patient Number"))
        cast_dict = {old: self.cgm_schema_polars[old] for old in self.cgm_schema_polars if old in cgm_df.columns}
        single_subject_cgm_data = cgm_df.cast(cast_dict,strict=False)
        single_subject_cgm_data = single_subject_cgm_data.with_columns(pl.col("DI (Eng)").str.replace("Data not available","data not available"))

        if daily:
            single_subject_cgm_data = single_subject_cgm_data.group_by_dynamic("Date",every="1d").agg(pl.all())
        return single_subject_cgm_data.with_row_index("cgm_event_order")
    
    def create_all_cgm_data(self,subject_ids: Optional[list[str]] = None) -> pl.DataFrame:
        if subject_ids is None:
            subject_ids = self.df_metadata["Patient Number"].to_list()
        cgm_dfs = [self.load_single_subject_cgm_data(subject_id) for subject_id in subject_ids]
        return cgm_dfs
    
    def get_single_subject_cgm_data(self,subject_id:str) -> pl.DataFrame:
        return self.cgm_df.filter(pl.col("Patient Number")==subject_id)
    
    def combine_metadata_and_cgm_data(self,subject_id:str) -> pl.DataFrame:
        cgm_data = self.get_single_subject_cgm_data(subject_id)
        metadata = self.df_metadata.filter(pl.col("Patient Number")==subject_id)
        return metadata.join(cgm_data,on="Patient Number",how="left")
    
    def get_random_subject_id(self) -> str:
        subject_list = self.df_metadata["Patient Number"].to_list()
        return random.choice(subject_list)
    
    def get_random_subject_cgm_data(self,resolution:Optional[Literal["1d","12h","6h","3h","1h"]]=None) -> pl.DataFrame:
        subject_list = self.df_metadata["Patient Number"].to_list()
        random_subject_id=self.get_random_subject_id()
        if resolution is None:
            return self.combine_metadata_and_cgm_data(random_subject_id)
        else:
            return self.get_single_subject_cgm_data_at_resolution(random_subject_id,resolution)

    def get_single_subject_cgm_data_at_resolution(self,subject_id:str,resolution:Literal["1d","12h","6h","3h","1h"]) -> pl.DataFrame:
        aggr_df = self.combine_metadata_and_cgm_data(subject_id)
        return aggr_df.group_by_dynamic("Date",every=resolution).agg((pl.col("Date").last()-pl.col("Date").first()).alias("Duration"),pl.col(self.cgm_columns).exclude("Date","Patient Number"),pl.col(self.metadata_columns).first())


    def get_single_subject_food_events(self,subject_id:str,before_offset:Literal["-1d","-12h","-6h","-3h","-1h"]="-3h",after_offset:Literal["1d","12h","6h","3h","1h"]="3h") -> pl.DataFrame:
        cgm_data = self.get_single_subject_cgm_data(subject_id)
        food_events = cgm_data.filter(pl.col("DI (Eng)").is_not_null()).select(pl.col("Date"),pl.col("Patient Number"),pl.col("DI (Eng)")).with_columns(pl.col("Date").dt.offset_by(before_offset).alias("start_interval"),pl.col("Date").dt.offset_by(after_offset).alias("end_interval")).sort("Date")
        return food_events.with_row_index("event_order")
    
    def get_single_subject_events_cgm_data(self, subject_id: str, before_offset: Literal["-1d","-12h","-6h","-3h","-1h"] = "-3h", after_offset: Literal["1d","12h","6h","3h","1h"] = "3h") -> pl.DataFrame:
        food_events = self.get_single_subject_food_events(subject_id, before_offset, after_offset)
        single_subject_cgm_data = self.get_single_subject_cgm_data(subject_id)
        
        event_df = pl.concat([
            single_subject_cgm_data.filter(
                (pl.col("Date") >= food_events["start_interval"][i]) & 
                (pl.col("Date") <= food_events["end_interval"][i])
            ).select(
                pl.col("Date"),
                pl.col("CGM"),
                pl.col("Patient Number")
            ).with_columns(
                pl.lit(food_events["event_order"][i]).alias("event_order"),
                pl.lit(f"food_event_{i}_{single_subject_cgm_data['Patient Number'][0]}").alias("event_id"),
                pl.lit(food_events["DI (Eng)"][i]).alias("event_description"),
                pl.lit(food_events["Date"][i]).alias("event_date")
            ) for i in range(food_events.shape[0])
        ]).with_columns([
            (pl.col("event_date") - pl.col("Date") > 0).alias("is_before_food_event"),
            pl.when(pl.col("event_date").dt.hour() < 10).then(pl.lit("Breakfast"))
            .when(pl.col("event_date").dt.hour() < 15).then(pl.lit("Lunch"))
            .otherwise(pl.lit("Dinner")).alias("event_type")
        ])

        grouped_event_df = event_df.group_by("event_id").agg([
            pl.col("Date"),
            pl.col("CGM"),
            pl.col("is_before_food_event"),
            pl.col("CGM").len().alias("cgm_length"),
            pl.col(["Patient Number", "event_description", "event_date", "event_order", "event_type"]).first()
        ])

        grouped_event_df = grouped_event_df.with_columns([
            pl.col("Date").list.first().alias("event_start_date"),
            pl.col("Date").list.last().alias("event_end_date"),
            (pl.col("Date").list.last() - pl.col("Date").list.first()).alias("event_duration")
        ])

        return grouped_event_df.sort(["event_order", pl.col("Date").list.first()])
    
    def get_all_food_events(self,before_offset:Literal["-1d","-12h","-6h","-3h","-1h"]="-3h",after_offset:Literal["1d","12h","6h","3h","1h"]="3h") -> pl.DataFrame:
        events = []
        for subject_id in self.df_metadata["Patient Number"].to_list():
            events.append(self.get_single_subject_food_events(subject_id,before_offset,after_offset))
        df = pl.concat(events)
        #reset the event_order index
        return df

    
    def get_all_events_cgm_data(self,before_offset:Literal["-1d","-12h","-6h","-3h","-1h"]="-3h",after_offset:Literal["1d","12h","6h","3h","1h"]="3h") -> pl.DataFrame:
        events = []
        for subject_id in self.df_metadata["Patient Number"].to_list():
            events.append(self.get_single_subject_events_cgm_data(subject_id,before_offset,after_offset))
        return pl.concat(events).select(pl.all().exclude("event_order")).with_row_index("event_order")
    def get_cgm_data_at_resolution(self, resolution: Literal["1d", "12h", "6h", "3h", "1h"] = "1d") -> pl.DataFrame:
        """
        Transform CGM data into specified time resolution buckets.
        
        Args:
            resolution: Time resolution for aggregation. One of "1d", "12h", "6h", "3h", "1h"
        
        Returns:
            pl.DataFrame: Aggregated CGM data with columns:
                - Patient Number
                - Date (start of time bucket)
                - CGM (list of readings)
                - cgm_time_stamp (list of timestamps)
                - count (number of readings)
                - cgm_event_order (list of original indices)
        """
        df =  self.cgm_df.group_by_dynamic(
            "Date",
            every=resolution,
            group_by=["Patient Number"]
        ).agg([
            pl.col("CGM"),
            pl.col("Date").alias("cgm_time_stamp"),
            pl.col("Date").count().alias("count"),
            pl.col("cgm_event_order")
        ])

        return df.with_row_index(f"{resolution}_event_order")