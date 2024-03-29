from collections import Counter
import datetime as dt
from itertools import chain
import re
import requests
from typing import Dict, List, Tuple, Union

import pandas as pd


class CensusDatasetSource:
    def __init__(self, base_api_call: str, media_type: str = "json"):
        self.base_api_call = base_api_call
        self.media_type = media_type

    def get_detail_url(self, detail_type: str) -> str:
        return f"{self.base_api_call}/{detail_type}.{self.media_type}"

    @property
    def variables_url(self):
        return self.get_detail_url(detail_type="variables")

    @property
    def examples_url(self):
        return self.get_detail_url(detail_type="examples")

    @property
    def sorts_url(self):
        return self.get_detail_url(detail_type="sorts")

    @property
    def geographies_url(self):
        return self.get_detail_url(detail_type="geography")

    @property
    def tags_url(self):
        return self.get_detail_url(detail_type="tags")

    @property
    def groups_url(self):
        return self.get_detail_url(detail_type="groups")

    def get_url_response(self, url: str) -> Dict:
        api_call = re.sub("\.html$", ".json", url)
        resp = requests.get(api_call)
        if resp.status_code == 200:
            resp_json = resp.json()
            return resp_json
        else:
            print(f"Failed to get a valid response; status code: {resp.status_code}")
            return None

    @property
    def variables_df(self) -> None:
        variables_resp_json = self.get_url_response(self.variables_url)
        variables_df = pd.DataFrame(variables_resp_json["variables"]).T
        variables_df.index.name = "variable"
        variables_df = variables_df.reset_index()
        variables_df["predicateOnly"] = variables_df["predicateOnly"].fillna(False)
        variables_df["values"] = variables_df["values"].fillna({})
        return variables_df
    
    @property
    def geographies_df(self) -> None:
        geo_resp_json = self.get_url_response(self.geographies_url)
        geographies_df = pd.DataFrame(geo_resp_json["fips"])
        return geographies_df
    
    @property
    def groups_df(self) -> None:
        groups_resp_json = self.get_url_response(self.groups_url)
        groups_df = pd.DataFrame(groups_resp_json["groups"])
        return 

class CensusAPICatalog:
    def __init__(self):
        self.set_data_catalog_json()
        self.set_dataset_metadata()

    def set_data_catalog_json(self) -> pd.DataFrame:
        url = "https://api.census.gov/data.json"
        resp = requests.get(url)

        if resp.status_code == 200:
            data_catalog_json = resp.json()
            self.data_catalog_json = data_catalog_json
        else:
            raise Exception(f"Failed to get a valid response; status_code: {resp.status_code}")

    def set_dataset_metadata(self) -> None:
        if "dataset" in self.data_catalog_json.keys():
            datasets = self.data_catalog_json["dataset"]
            print(f"Elements in Census data catalog datasets attr: {len(datasets)} ")
            df_list = []
            df_shape_list = []
            for dataset in datasets:
                df = pd.json_normalize(dataset)
                df_list.append(df)
                df_shape_list.append(df.shape)
            full_df = pd.concat(df_list)
            full_df = full_df.reset_index(drop=True)
            full_df["modified"] = pd.to_datetime(full_df["modified"])

            col_order = [
                "title",
                "identifier",
                "modified",
                "temporal",
                "bureauCode",
                "programCode",
                "description",
                "keyword",
                "spatial",
                "c_vintage",
                "c_dataset",
                "c_geographyLink",
                "c_variablesLink",
                "c_tagsLink",
                "c_examplesLink",
                "c_groupsLink",
                "c_sorts_url",
                "c_documentationLink",
                "c_isAggregate",
                "c_isCube",
                "c_isAvailable",
                "c_isTimeseries",
                "c_isMicrodata",
                "@type",
                "accessLevel",
                "distribution",
                "license",
                "references",
                "contactPoint.fn",
                "contactPoint.hasEmail",
                "publisher.@type",
                "publisher.name",
                "publisher.subOrganizationOf.@type",
                "publisher.subOrganizationOf.name",
                "publisher.subOrganizationOf.subOrganizationOf.@type",
                "publisher.subOrganizationOf.subOrganizationOf.name",
            ]            
            full_df = full_df[col_order].copy()
            distribution_df = pd.json_normalize(full_df["distribution"].str[0])
            distribution_df.columns = [f"distribution_{col}" for col in distribution_df.columns]
            full_df = pd.merge(
                left=full_df, right=distribution_df, how="left", left_index=True, right_index=True
            )
            full_df = full_df.sort_values(by="modified", ascending=False, ignore_index=True)
            col_fix_map = {el: el.replace("@", "") for el in full_df.columns}
            full_df = full_df.rename(columns=col_fix_map)
            self.dataset_metadata = full_df
        else:
            raise Exception(f"field 'dataset' not found in data_catalog response")
            
    def get_counts_of_nested_data_elements(self, key: str) -> List[Tuple]:
        datasets = self.data_catalog_json["dataset"]
        labels = [d[key] for d in datasets]
        label_counts = Counter(chain.from_iterable(labels))
        label_counts = sorted(label_counts.items(), key=lambda x: x[1], reverse=True)
        return label_counts
            
    def get_dataset_source(self, identifier: str, media_type: str = "json") -> CensusDatasetSource:
        base_api_call = self.dataset_metadata.loc[
            self.dataset_metadata["identifier"] == identifier, "distribution_accessURL"
        ].values[0]
        return CensusDatasetSource(base_api_call=base_api_call, media_type=media_type)
    
    def standardize_datetime_str_repr(self, datetime_obj: Union[str, dt.datetime]) -> str:
        if isinstance(datetime_obj, str):
            datetime_obj = dt.datetime.strptime(datetime_obj, "%Y-%m-%dT%H:%M:%S.%fZ")
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    

class CensusAPIHandler:
    def __init__(self):
        self.catalog = CensusAPICatalog()
        self.time_of_check = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self.prepare_dataset_metadata_df()

    def prepare_dataset_metadata_df(self):
        metadata_df = self.catalog.dataset_metadata.copy()
        colname_fixes = {
            'identifier': 'identifier',
            'title': "title",
            'description': "description",
            'modified': "modified",
            'c_vintage': "vintage",
            'distribution_accessURL': "distribution_access_url",
            'c_geographyLink': "geography_link",
            'c_variablesLink': "variables_link",
            'c_tagsLink': "tags_link",
            'c_examplesLink': "examples_link",
            'c_groupsLink': "groups_link",
            'c_sorts_url': "sorts_url",
            'c_dataset': "dataset",
            'spatial': "spatial",
            'temporal': "temporal",
            'bureauCode': "bureauCode",
            'programCode': "programCode",
            'keyword': "keyword",
            'c_isMicrodata': "is_microdata",
            'c_isAggregate': "is_aggregate",
            'c_isCube': "is_cube",
            'c_isAvailable': "is_available",
            'c_isTimeseries': "is_timeseries",
            'accessLevel': "access_level",
            'license': "license",
            'type': "type",
            'publisher.name': "publisher_name",
            'publisher.type': "publisher_type",
            'contactPoint.fn': "contact_point_fn",
            'contactPoint.hasEmail': "contact_point_email",
            'distribution_type': "distribution_type",
            'distribution_mediaType': "distribution_media_type",
            'references': "references",
            'c_documentationLink': "documentation_link",
        }
        metadata_df = metadata_df[colname_fixes.keys()].copy()
        metadata_df = metadata_df.rename(columns=colname_fixes)

        bool_cols = ["is_microdata", "is_aggregate", "is_cube", "is_timeseries", "is_available"]
        for bool_col in bool_cols:
            metadata_df[bool_col] = metadata_df[bool_col].fillna(False).astype(bool)

        mask = metadata_df["vintage"].isnull()
        metadata_df["vintage"] = metadata_df["vintage"].fillna(-1).astype(int).astype(str)
        metadata_df.loc[mask, "vintage"] = None
        metadata_df["time_of_check"] = self.time_of_check
        self.metadata_df = metadata_df.copy()

