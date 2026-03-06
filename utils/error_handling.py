# Copyright (c) 2024-2026 Steve Young
# Licensed under the MIT License
import copy

class ConfigError(Exception):
    # Errors in the config.py file
    pass

class ParseError(Exception):
    #Errors in parsing (generally documents not meeting parsing assumptions)
    pass

class InputError(Exception):
    #Errors in the input provided to the program.
    pass

def InputWarning(explanation):
    #Issues to warn about, but not stop execution.
    print("Input warning:")
    print(explanation + '\n\n')

def ParseWarning(explanation):
    #Issues to warn about, but not necessary to stop execution.
    print("Parsing Warning:")
    print(explanation + '\n\n')

def log_parsing_correction(file_path, correction_type, details, logfile_path=None):
    """
    Log instances where parsing corrections were applied for monitoring.
    
    Args:
        file_path (str): Path to the file where correction was applied
        correction_type (str): Type of correction (e.g., 'malformed_annex_structure')
        details (str): Description of the correction applied
        logfile_path (str, optional): Path to document issues logfile for structured logging
    """
    # Console output for immediate visibility
    print(f"[PARSING CORRECTION] {correction_type}")
    if file_path:
        print(f"  File: {file_path}")
    print(f"  Details: {details}")
    print()
    
    # Structured logging to document issues file if provided
    if logfile_path:
        import os
        import json
        from datetime import datetime, UTC
        
        log_entry = {
            'issue_type': 'parsing_correction',
            'correction_type': correction_type,
            'issue': details,
            'file_path': file_path,
            'timestamp': str(datetime.now(UTC))
        }
        
        # Ensure logfile directory exists
        os.makedirs(os.path.dirname(logfile_path), exist_ok=True)
        
        # Append to logfile
        with open(logfile_path, 'a') as f:
            f.write(json.dumps(log_entry, indent=2))
            f.write('\n')
    
class ModelError(Exception):
    #Errors in interacting with the AI models.
    pass
    
def CheckVersion(parsed_content):
    if 'document_information' in parsed_content.keys():
        if not 'version' in parsed_content['document_information'].keys() or float(parsed_content['document_information']['version']) < 0.3:
            ParseError("Parsed document format in unsupported version.")
            exit(1)
    else:
        ParseError("No document_information found.")
        exit(1)