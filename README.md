import os
import sys
import re
import time
import pandas
import yaml
import json
import csv
import pprint
import argparse
import uuid
import logging
import pandas as pd

from address_engine import GoogleMaps, BingMaps, NominatimGeocoder, AusPost
from address_normalization import normalize_address, denormalize_address, lookup_normalized_address_component, extract_postbox
from address_standardization import map_to_standard_old, bingmaps_to_standard, nomiatium_to_standard, pas_split_address, map_to_original, map_to_standard
from address_fuzzy_nlp import levenshtein_similarity, calculate_similarity_ml, jaccard_similarity, determine_similarity_level, determine_similarity_level_ml, run_fuzzy
from pii_handler import redact_columns
from llm_wrapper import ChatGPT


logging.basicConfig(level=logging.INFO, filename="output/address_validation.log", format='%(asctime)s: %(levelname)s: %(name)s:  %(message)s')
logger = logging.getLogger(__name__)
logger.info("#####################################################################################")
logger.info("################################# Address Validation Engine #########################")

class AddressValidation(object):
    def __init__(self,
                 source_system=None,
                 source_data=None,
                 source_metadata=None,
                 key=None,
                 namespace=None,
                 instant_mode=False,
                 address=None,
                 nrows=None,
                 output_filepath=None,
                 api=None,
                 # float_precision: Literal["high", "legacy"] | None = None,
                 column_list_to_redact=None):
        # if api not in ["googlemaps", "bingmaps", "nominatim"]:
        #     raise ValueError("Invalid value for 'api'. Must be either 'nominatim', or 'googlemaps' or 'bingmaps'.")
        self.api = "nominatim" if api is None else api
        # self.api = api
        self.source_system = source_system
        self.source_data = source_data
        self.source_metadata = source_metadata
        self.key = None if ([key] if isinstance(key, str) else key) is None else ([key] if isinstance(key, str) else key)
        self.namespace = "myserver.namespace.sample" if key is None else namespace.lower()
        self.instant_mode = instant_mode
        self.address = address if instant_mode else None
        self.nrows = None if nrows is None else nrows
        self.output_filepath = "output/validation_result_default.csv" if output_filepath is None else output_filepath
        self.guid = None if key is None else key  # TODO: to be fixed
        self.api_converted_to_standard = None
        self.column_list_to_redact = column_list_to_redact
        # self.regex_postbox = SETTINGS["regex"]["po_box"] if SETTINGS["regex"]["po_box"] is None else r'(?i)(P O BOX [\d]+|RSD [\d]+|PO BOX [\d]+)'
        self.regex_postbox = r'(?i)(P O BOX [\d]+|RSD [\d]+|PO BOX [\d]+)'

    def check_args(self):
        message_details = f"\nChecking arguements provided...\n"
        logger.info(message_details) or print(message_details)
        if self.instant_mode and self.address is None:
            message_details = "\n[Critical Error] \nReason: incomplete arguments (address, i, instant_mode, api)"
            logger.error(message_details) or print(message_details)
            sys.exit(message_details)
            return False
        elif self.instant_mode is False and (self.source_system is None or self.source_data is None or self.source_metadata is None or self.key is None):
            msg = "\n[Critical Error] \nReason: incomplete arguments (source_system, source_data, source_metadata, key, instant_mode & address)"
            # message_details = msg
            logger.error(message_details) or print(message_details)
            sys.exit(message_details)
            return False
        else:
            return True

    def load_address_component_reference(self, address_component={"postcode", "conuntry_code", "state", "city", "suburb","gpid"}):
        message_details = f"Loading address component reference [address_component: {address_component}]..."
        logger.info(message_details)

    def _get_geocode(self):
        pass

    def _get_dpid(self):
        pass

    # def cross_check_address(self, corrected_address, reference_base="bingmaps"):
    #     message_details = f"\nPerforming cross_check_address using {reference_base} [{corrected_address}] ..."
    #     logger.info(message_details)  # or print(message_details)

    def dry_quick(self, df=None):
        # TODO:  if df is None or not (isinstance(df, pandas.DataFrame)) or not (isinstance(df, )) or len(df) == 0:
        if df is None or len(df) == 0:
            print("Reason: The provided address data in dataframe is not valid.")
            sys.exit()

        CGoogleMaps = GoogleMaps(api_key=None, base_url=None)
        # ret = CGoogleMaps.suggest_address_list(pd.read_csv("output/experian_address_mockup.csv", delimiter=",", nrows=None)["Original address"])
        ret = CGoogleMaps.suggest_address_list(df)
        pprint.pprint(ret)
        return (ret)

    def load_address_metadata(self, source_metadata=None):
        message_details = f"Loading address source_metadata: {source_metadata}]..."
        logger.info(message_details)  # or print(message_details)
        with open(source_metadata, 'r') as f:
            address_metadata_loaded = json.load(f)  # data is a Python dictionary
        message_details = f"\nAddress metadata [{source_metadata}] loaded:\n [{address_metadata_loaded}]"
        return address_metadata_loaded

    def load_address_data(self, source_data=None, column_list_to_redact=None):
        message_details = f"Loading address data [Source system: {source_data}]..."
        logger.info(message_details)  # or print(message_details)
        address_data_loaded_native = pd.read_csv(source_data, delimiter=",", nrows=self.nrows)  # TODO: to be fixed

        if column_list_to_redact is not None:
            try:
                address_data_loaded_redacted = redact_columns(address_data_loaded_native,column_list_to_redact=column_list_to_redact)  # TODO: case detect for columns
                print(address_data_loaded_redacted)
            except ValueError as e:
                message_details = f"{e}"
                logger.error(message_details) or print(message_details)
                sys.exit()

        address_data_loaded_redacted.columns = address_data_loaded_redacted.columns.str.lower()
        message_details = f"\nAddress data [{source_data}] loaded:\n [{address_data_loaded_redacted}]"
        return address_data_loaded_redacted

    def main(self):
        start_time = time.time()
        if self.check_args() is False:
            print("Reason: incomplete arguments (source_data, source_metadata, key, instant_mode & address)")
            sys.exit()

        """ MAPS SERVICES"""
        CGoogleMaps = GoogleMaps(api_key=None, base_url=None)
        CBingMaps = BingMaps(api_key=None, base_url=None)
        CNominatim = NominatimGeocoder(api_key=None, base_url=None)
        CAusPost = AusPost(api_key=None, password=None, base_url=None)

        if self.instant_mode:
            source_data = self.source_data
            source_metadata = self.source_metadata
            guid = str(uuid.uuid4())
            address = self.address

            api_converted_to_standard = None
            validation_result, suggested_address_from_api, suggested_coordinates_from_api, place_id_from_api, address_components_from_api = CNominatim.validate_address(address)            
            # if validation_result == "Invalid" and self.api == "googlemaps":
            if self.api == "googlemaps":
                suggestion_result_api, suggested_address_from_api, suggested_coordinates_from_api, place_id_from_api, address_components_from_api = CGoogleMaps.suggest_address(address)

                if suggestion_result_api == "Invalid":
                    print("Reason: Service unreachable")
                    sys.exit()

                # if len(address_components_from_api) < 1:
                #     print("Reason: Address component invalid due to e.g. Road, Street, Highway")
                #     sys.exit()

                # api_converted_to_standard = map_to_standard(address_from_api=address_components_from_api, standard=True, api=self.api)
                # """ Standard To Original (Minerva)"""
                # standard_address_map_api = 'metadata/standard_address_map_googlemaps.json'
                # converted_back_to_original_minerva = map_to_original(
                #     standard_from_api=api_converted_to_standard,
                #     metadata=standard_address_map_api,
                #     node_in_metadata='standard_to_minerva',
                #     combined_street_address=False)
                # message_details = f"\nStandard address format converted back to the original format [standard_to_minerva]:"
                # logger.info(message_details) or print(message_details)
                # pprint.pprint(converted_back_to_original_minerva, sort_dicts=False)

            # elif validation_result == "Invalid" and self.api == "bingmaps":
            elif self.api == "bingmaps":
                suggestion_result_api, suggested_address_from_api, suggested_coordinates_from_api, place_id_from_api, address_components_from_api = CBingMaps.suggest_address(
                    address)
                api_converted_to_standard = "api_converted_to_standard for Googlemap for now"
                if suggestion_result_api == "Invalid":
                    print("Reason: Service unreachable")
                    sys.exit()

            if address_components_from_api == "Invalid" or len(address_components_from_api) < 1:
                print("Reason: Address component insufficient due to e.g. Road, Street, Highway")
                sys.exit()

            api_converted_to_standard = map_to_standard(address_from_api=address_components_from_api, standard=True, api=self.api)
            """
            check either street/unit/subpremise number and street address is missing 
            """
            valid_street_address = True if (api_converted_to_standard['street_number'] != "street_number_na" and api_converted_to_standard['street'] != "street_na") else False

            """Fuzzy Matching"""
            similarity_ratio = levenshtein_similarity(source=(address.lower()).replace(",", ""), target=(suggested_address_from_api.lower()).replace(",", ""))
            similarity_level = determine_similarity_level(similarity_ratio)

            # from llm_wrapper import ChatGPT
            prompt = f"Find out any info as much as possible for the following address: {suggested_address_from_api}"
            chatgpt = ChatGPT(engine="gpt-3.5-turbo-instruct", max_tokens=1024, n=1, stop=None, temperature=0.5)
            gptwb_result = chatgpt.chatgpt_wrapper(prompt=prompt)
            gptwb_result = "\n".join(gptwb_result.split("."))

            print (f"Instant Mode Result:")
            import urllib.parse
            instant_mode_result = {
                "GUID": guid,
                "Original address": address,
                "Validation Result": validation_result,
                "Suggested Address": suggested_address_from_api,
                "Valid unit/street number and address": str(valid_street_address),
                "Similarity Ratio": similarity_ratio,
                "Similarity Level": similarity_level,
                "API Converted to Standard": "api_converted_to_standard for Googlemap for now" if api_converted_to_standard is None else api_converted_to_standard,
                "Instant Mode": str(self.instant_mode),
                "API Service": str(self.api),
                "Note": "Nominatim API may not provide 'street number' so mapping back to original is not accurate. Use googlemaps/bingmaps API instead." if str(self.api) == "nominatim" else "None",
                "Google Maps": f"<a href=\"https://www.google.com/maps/place/{urllib.parse.quote_plus(suggested_address_from_api)}\">Google Maps</a>",
                "GTP Workbook (LLM)": "\n".join(gptwb_result.splitlines()),
                # "GTP Workbook2": "\n".join(gptwb_result.split("\n"))
            }
            instant_mode_result = {k.upper(): v for k, v in instant_mode_result.items()}
            pprint.pprint(instant_mode_result, sort_dicts=False)
            print(json.dumps(instant_mode_result, indent=4))
            with open(os.path.join("output/", "instant_mode_result" + ".json"), "a",
                      encoding="utf-8") as f:  # TODO: to fix it
                json.dump(instant_mode_result, f, indent=4)
                f.write(",\n")
            # exit(0)
        else:
            """ Load Metadata first ensure PII not be loaded without control applied """
            address_metadata_loaded = self.load_address_metadata(self.source_metadata)

            """ Get column_list_to_redact from Metadata where 'address_attribute' == 'no' and exclude KEY"""
            # Filter the schema list by the condition "address_attribute" = "no" and extract the "key" values
            """ key should be UPPER vs value to be lower"""
            column_list_to_redact = [d['key'].upper() for d in address_metadata_loaded['schema'] if d['address_attribute'] == 'no']

            """
            Perform redact setting "before" data loading
            key_to_exclude: key (as string) or list of key (as list) to be convert Upper (or lower). #TODO: to be fixed!!!
            """
            key_to_exclude = self.key.upper() if isinstance(self.key, str) else ([x.upper() for x in self.key] if isinstance(self.key, list) else TypeError("The data type is not supported"))
            column_list_to_redact = set(column_list_to_redact).difference(key_to_exclude)

            """ Load Data and PII Masking """
            address_data_loaded = self.load_address_data(self.source_data, column_list_to_redact)
            column_mapping = {d['key']: d['value'] for d in address_metadata_loaded['schema']}
            address_data_loaded_cmapped = address_data_loaded.rename(columns=column_mapping)

            """ 
            Check key columns are in the loaded data
            column name to be lower case 
            key should be UPPER vs value to be lower
            """
            column_name_as_value = self.key.lower() if isinstance(self.key, str) else ([x.lower() for x in self.key] if isinstance(self.key, list) else TypeError("The data type is not supported"))
            if not all(col in address_data_loaded.columns for col in ([column_name_as_value] if isinstance(column_name_as_value, str) else column_name_as_value)):
                message_details = f"Critical Error: Key {column_name_as_value} is not available"
                print(message_details)
                sys.exit()

            # self.load_address_component_reference() #TODO: to be fixed
            # address_normalized = denormalize_address()
            # address_to_validate = lookup_normalized_address_component(address_normalized)

            for index, row in address_data_loaded_cmapped.iterrows():
                print(f"\nProcessing record [{index + 1}/{len(address_data_loaded_cmapped)}]...")
                guid = str(uuid.uuid4())

                # value_in_key_column = '|'.join( row[column_name_as_value] )
                value_in_key_column = '|'.join(f"{column_name_as_value}_IS_NONE" if row[column_name_as_value] is None else str(row[column_name_as_value]))

                """ Normaliaze Address"""
                address_normalized_original = normalize_address(row, address_metadata_loaded)

                """
                extract_postbox()
                - regex = r'(PO BOX [\d]+|RSD [\d]+)'
                - subpremise
                """

                # post_box, address_normalized = extract_postbox(address_normalized_original, regex_to_pobx = r'(?i)(P O BOX [\d]+|RSD [\d]+|PO BOX [\d]+)')  # TODO: to be fixed. included into subpremise

                post_box, address_normalized = extract_postbox(address=address_normalized_original, regex_postbox=self.regex_postbox)  # TODO: to be fixed. included into subpremise

                message_details = f"Original address denormalized to [{address_normalized}] before validation."
                logger.info(message_details) or print(message_details)

                try:
                    validation_result = "Not validated yet"
                    if self.api is not None and self.api in ["nominatim", "googlemaps", "bingmaps"]:
                        validation_result, suggested_address_from_api, suggested_coordinates_from_api, place_id_from_api, address_components_from_api = CNominatim.validate_address(address_normalized)
                        if self.api == "nominatim":
                            if validation_result == "Invalid":
                                print(f"No address to proceed")
                                continue
                            else:
                                suggestion_result_api = nomiatium_to_standard(address_components_from_api)
                                standard_address_map_api = 'metadata/metadata/standard_address_map_nominatim.json.json'
                        elif self.api == "googlemaps":
                            suggestion_result_api, suggested_address_from_api, suggested_coordinates_from_api, place_id_from_api, address_components_from_api = CGoogleMaps.suggest_address(address_normalized)
                            standard_address_map_api = 'metadata/standard_address_map_googlemaps.json'
                        elif self.api == "bingmaps":
                            suggestion_result_api, suggested_address_from_api, suggested_coordinates_from_api, place_id_from_api, address_components_from_api = CBingMaps.suggest_address(address_normalized)
                            standard_address_map_api = 'metadata/standard_address_map_bingmaps.json'
                        else:
                            pass

                        if suggestion_result_api == "Invalid":
                            message_details = f"\nReason: Service unreachable [API: {self.api}] or Address component insufficient due to e.g. Road, Street, Highway."
                            logger.info(message_details) or print(message_details)
                            continue

                        api_converted_to_standard = map_to_standard(address_from_api=address_components_from_api, standard=True, api=self.api)
                        """
                        check either street/unit/subpremise number and street address is missing 
                        """
                        # invalid_street_address = True if (api_converted_to_standard['street_number'] == "street_number_na" and api_converted_to_standard['street'] == "street_na") else False
                        valid_street_address = True if (api_converted_to_standard['street_number'] != "street_number_na" and api_converted_to_standard['street'] != "street_na") else False
                        self.api_converted_to_standard = api_converted_to_standard


                        message_details = f"\nSuggested correct {self.api} address converted to standard address format: \n{api_converted_to_standard}"
                        logger.info(message_details) or print(message_details)

                        """Fuzzy Matching"""
                        similarity_ratio = levenshtein_similarity(source=(address_normalized.lower()).replace(",", ""),
                                                                  target=(suggested_address_from_api.lower()).replace(",", ""))
                        # similarity_ratio = jaccard_similarity(source=(address_normalized.lower()).replace(",", ""), target=(suggested_address_from_api.lower()).replace(",", ""))
                        similarity_level = determine_similarity_level(similarity_ratio)

                        if suggestion_result_api == "Invalid":
                            converted_back_to_original_pas_split = f"Invalid address [{self.api}]"
                            converted_back_to_original_sf = f"Invalid address [{self.api}]"
                            converted_back_to_original_cmdm = f"Invalid address [{self.api}]"
                            converted_back_to_original_minerva = f"Invalid address [{self.api}]"
                        elif suggestion_result_api == "Valid" and not valid_street_address:
                            converted_back_to_original_pas_split = f"invalid_street_address"
                            converted_back_to_original_sf = f"invalid_street_address"
                            converted_back_to_original_cmdm = f"invalid_street_address"
                            converted_back_to_original_minerva = f"invalid_street_address"
                        elif suggestion_result_api == "Valid" and valid_street_address:
                            """ Standard To Original (PAS): Option 1 (split)  """
                            converted_back_to_original_pas_split = pas_split_address(suggested_address_from_api)
                            message_details = f"\nStandard address format converted back to the original format [converted_back_to_original_pas_split]: \n{converted_back_to_original_pas_split}"
                            logger.info(message_details) or print(message_details)

                            """ Standard To Original (SF)"""
                            converted_back_to_original_sf = map_to_original(standard_from_api=api_converted_to_standard,
                                                                            metadata=standard_address_map_api,
                                                                            node_in_metadata='standard_to_salesforce',
                                                                            combined_street_address=True)
                            message_details = f"\nStandard address format converted back to the original format [converted_back_to_original_sf]: \n{converted_back_to_original_sf}"
                            logger.info(message_details) or print(message_details)

                            """ Standard To Original (Minerva)"""
                            converted_back_to_original_minerva = map_to_original(standard_from_api=api_converted_to_standard,
                                                                                 metadata=standard_address_map_api,
                                                                                 node_in_metadata='standard_to_minerva',
                                                                                 combined_street_address=False)
                            message_details = f"\nStandard address format converted back to the original format [standard_to_minerva]: \n{converted_back_to_original_minerva}"
                            logger.info(message_details) or print(message_details)

                            """ Standard To Original (CMDM)"""
                            converted_back_to_original_cmdm = map_to_original(standard_from_api=api_converted_to_standard,
                                                                              metadata=standard_address_map_api,
                                                                              node_in_metadata='standard_to_cmdm',
                                                                              combined_street_address=False)
                            message_details = f"\nStandard address format converted back to the original format [standard_to_cmdm]: \n{converted_back_to_original_cmdm}"
                            logger.info(message_details) or print(message_details)
                        else:
                            pass
                    else:
                        converted_back_to_original_pas_split = f"TODO: to be fixed [{self.api}]"
                        converted_back_to_original_sf = f"TODO: to be fixed [{self.api}]"
                        converted_back_to_original_cmdm = f"TODO: to be fixed [{self.api}]"
                        converted_back_to_original_minerva = f"TODO: to be fixed [{self.api}]"

                    validation_result_bundled = {
                        "KEY": value_in_key_column,
                        "Original address": address_normalized_original,
                        "Validation Result": validation_result,
                        "Suggested Address": suggested_address_from_api if post_box is None else f'{post_box} {suggested_address_from_api}',
                        "Valid unit/street number and address": f"{str(valid_street_address)} {'' if post_box is None else '[PO BOX added instead]'}",

                        "Similarity Ratio": similarity_ratio,
                        "Similarity Level": similarity_level,

                        "Suggested Coordinates": suggested_coordinates_from_api,
                        "Suggested Place ID": place_id_from_api,
                        "Suggested DPID": "TBD",
                        "converted_back_to_original_SYD_pas_split": converted_back_to_original_pas_split,
                        # "converted_back_to_original_pas": converted_back_to_original_pas,
                        "converted_back_to_original_sf": converted_back_to_original_sf,
                        "converted_back_to_original_cmdm": converted_back_to_original_cmdm,
                        "converted_back_to_original_minerva": converted_back_to_original_minerva,

                        "Column Name Key": self.key,
                        "Column Name Value": column_name_as_value,
                        "GUID": guid,  # str(uuid.uuid4())
                        "Namespace": self.namespace,
                        "Source System": self.source_system,
                        "Source Data": self.source_data,
                        "Source Metadata": self.source_metadata,
                        "Instant Mode": str(self.instant_mode),
                        "API Service": str(self.api),
                        "Note": "Nominatim API does not provide 'street number' so mapping back to original is not accurate. Use googlemaps/bingmaps API instead." if str(self.api) == "nominatim" else "None"
                    }
                    print(f"\nValidation Results Bundled:")
                    # logger.info(validation_result_bundled) or pprint.pprint(validation_result_bundled, sort_dicts=False)
                    logger.info(validation_result_bundled) or json.dumps(validation_result_bundled, indent=4)

                    with open(os.path.join("output/", os.path.splitext(os.path.basename(self.output_filepath))[0].strip() + ".json"), "a", encoding="utf-8") as f:  # TODO: to fix it
                        validation_result_bundled = {k.upper(): v for k, v in validation_result_bundled.items()}
                        json.dump(validation_result_bundled, f, indent=4)
                        f.write(",\n")

                    with open(self.output_filepath, "a", newline="") as csvfile:
                        validation_result_bundled = {k.upper(): v for k, v in validation_result_bundled.items()}
                        writer = csv.DictWriter(csvfile, fieldnames=validation_result_bundled.keys())
                        if index == 0:
                            writer.writeheader()
                        writer.writerow(validation_result_bundled)

                    key_place_id_mapped = {
                        "GUID": guid,
                        "Key Name": self.key,
                        "Key Column": column_name_as_value,
                        "Namespace": self.namespace,
                        "Place ID": place_id_from_api,
                        "Address": suggested_address_from_api,
                        "Address Components": address_components_from_api,
                        "API Service": str(self.api)
                    }

                    with open(os.path.join("output/", os.path.splitext(os.path.basename(self.output_filepath))[0].strip() + "_key_place_id_mapped_output.json"), "a", encoding="utf-8") as f:  # TODO: to fix it
                        key_place_id_mapped = {k.upper(): v for k, v in key_place_id_mapped.items()}
                        json.dump(key_place_id_mapped, f, indent=4)
                        f.write(",\n")
                    logger.info(key_place_id_mapped) or pprint.pprint(key_place_id_mapped, sort_dicts=False)
                except Exception as e:
                    message_details = f"GUID={0}, address={1}, message={2}".format(guid, address_normalized, e)
                    logger.error(message_details) or print(message_details)

        elapsed_time = time.time() - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        message_details = f"\nElapsed time: {int(hours)}:{int(minutes)}:{seconds:.2f}"
        logger.info(message_details) or print(message_details)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_system", type=str, help="Source system for source data. Example: 'minerva'")
    parser.add_argument("--source_data", type=str, help="Source address data to import. Example: 'bdax_address.csv'")
    parser.add_argument("--column_list_to_redact", type=str, help="list of columns to redact e.g. ['ADDRESSEE', 'Email']")
    parser.add_argument("--source_metadata", type=str, help="Source address metadata for source address data. Example: 'bdax_address.json'")
    parser.add_argument("--key", type=str, help="Unique key (column/field name) for address data. Example. 'Policy_Number'")
    parser.add_argument("--namespace", type=str, help="(Optional) Namespace with a combination of source_system.source_data. Example. 'minerva.party_addr'")
    parser.add_argument("--instant_mode", type=str, help="(Optional) Flag for Instant Mode")
    parser.add_argument("--address", type=str, help="(Optional)Single address to validate for Instant Mode")
    parser.add_argument("--nrows", type=int, help="(Optional) Number of record to read. None: max records")
    parser.add_argument("--output_filepath", type=str, help="Path to output dirname and filename")
    parser.add_argument("--api", type=str, choices=["googlemaps", "bingmaps", "nominatim"], default="nominatim", help="Address service for suggestion/correction. default='googlemaps'")

    args = parser.parse_args()
    Address_Validation = AddressValidation(source_system=args.source_system,
                                           source_data=args.source_data,
                                           column_list_to_redact=args.column_list_to_redact,
                                           source_metadata=args.source_metadata,
                                           key=args.key,
                                           namespace=args.namespace,
                                           instant_mode=args.instant_mode,
                                           address=args.address,
                                           nrows=args.nrows,
                                           output_filepath=args.nrows,
                                           api=args.api)

    # ### For Instant Mode release ###
    """
    https://www.google.com/maps/place/U+4%2F177+W+Coast+Hwy,+Scarborough+WA+6019/@-31.8894909,115.7548148,17z/data=!3m1!4b1!4m5!3m4!1s0x2a32a92b3297b9f5:0x733bd77905a2f60!8m2!3d-31.8894955!4d115.7573897?entry=ttu
    """
    # address = "2 Phillips St, Wallaroo SA 5556, New Zealand"
    # address = "U 4 177 ,,West Coast Hwy,,,,SCARBOROUGH,,WA,6019,AUSTRALIA"

    # """ test case: https://datatools.com.au/improve-existing-data/ """
    # address = "5 Cecil St MERRY ANDS NSW 2160"
    # address = "6 Balifor St Briton East Vic"
    # address = "9 Halford St Newtarm Qld"
    # address = "5 Sesil St Marylands Nsw"
    # # address = "43a Elisa St  Panorama Sa 5041 AU"  # correct: 43A Eliza Pl, Panorama SA 5041)
    # address = "43a Elisa pl   Sa 5041 AU"  # unable to verified by Google (missing Suburb), verifiedy by Bingmaps (ruturned unconfirmed suburb i.e. incorrect)
    #
    # address = "10 10 Wellingtn St Mosman Wa"  # incomplete (is street address correct)
    # address = "10 10 Wellington St Mosman WA"  # Bing unables to get street/unit number vs Google: correct

    # Reference: https: // www.byteplant.com / address - validator / google - address - validator - vs - byteplant - address - validator.html
    # address = "800 Victoria Ave, Rosebery NSW 2039, Australia"
    # address = "11 Piazza del Sant'Fffizio 00193 Roma"

    address = "5 Allsopp Ave Baulkham Hills 2153 NSW AU"

    # #1
    AVInstant = AddressValidation(address=address, instant_mode=True, api="googlemaps")  # nominatim, googlemaps, bingmaps
    AVInstant.main()


    ## $ python xaddress_validation.py --instant_mode=True --api='googlemaps' --address='400 George Street, Sydney NSW, 2000 New Zealand'
    ## "kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic address-data"

    # ### For Batch Mode release ###

    # #2
    # AV = AddressValidation(
    #                        source_data="data/address_sample_mockup.csv",
    #                        source_metadata="metadata/address_sample_mockup.json",
    #                        key="ID",
    #                        source_system="pas",
    #                        namespace="myserver.address.sample",
    #                        instant_mode=False,
    #                        nrows=None,
    #                        output_filepath="output/address_sample_mockup_output.csv",
    #                        api="googlemaps")  # nominatim, googlemaps, bingmaps
    # AV.main()
    #
    # #3
    # AV = AddressValidation(
    #     source_data="data/infoperian_address_mockup.csv",
    #     source_metadata="metadata/infoperian_address_mockup.json",
    #     key=["PARTY_KEY", "PRODUCT_SYSTEM_CODE"],
    #     source_system="infoperian",
    #     namespace="infoperian.test.rla.com.au",
    #     instant_mode=False,
    #     nrows=None,
    #     output_filepath="output/infoperian_address_mockup_output.csv",
    #     api="googlemaps")  # nominatim, googlemaps, bingmaps
    # AV.main()
    #

    # #4
    # AV = AddressValidation(
    #     source_data="data/golf_cc_address.csv",
    #     source_metadata="metadata/golf_cc_address.json",
    #
    #     key="KEY",
    #     source_system="pas",
    #     namespace="myserver.address.sample.golf",
    #     instant_mode=False,
    #     nrows=None,
    #     output_filepath="output/golf_cc_address_output.csv",
    #     api="googlemaps")  # nominatim, googlemaps, bingmaps
    # AV.main()

    # #5
    # AV = AddressValidation(
    #     source_data="data/poc/cid_wrong_address_short.csv",
    #     source_metadata="metadata/poc/cid_wrong_address.json",
    #     key="CID",
    #     source_system="CID System",
    #     namespace="myserver.address.sample.cid",
    #     instant_mode=False,
    #     nrows=10,
    #     output_filepath="output/cid_wrong_address_short_output.csv",
    #     api="bingmaps")  # nominatim, googlemaps, bingmaps
    # AV.main()


    # # # $ python address_validation.py --source_system='source_system='myserver_1' --source_data='data/address_sample_mockup.csv' --source_metadata='metadata/address_sample_mockup.json' --key='ID' --namespace='myserver.address.sample' --instant_mode=False   --nrows=1 --output_filepath='output/myserver.address.sample.csv' --api='googlemaps' --column_list_to_redact=['ADDRESSEE']

    # ## 4) Address Wash and Clean (scrubbing list of addresses):
    #
    # x = AV.dry_quick((pd.read_csv("data/poc/cid_wrong_address_short.csv", delimiter=",", nrows=None)["ADDRESS"]).head(100))
    # with open(os.path.join("output/", "x" + ".json"),
    #           "a", encoding="utf-8") as f:  # TODO: to fix it
    #     json.dump(x, f, indent=4)
    #     f.write(",\n")
    #
    # # with open("output/x.csv", "a", newline="\n\r") as csvfile:
    # #     writer = csv.DictWriter(csvfile, fieldnames=x.keys())
    # #     # if index == 0:
    # #     #     writer.writeheader()
    # #     writer.writerow(x)