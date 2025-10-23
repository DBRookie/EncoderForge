import re
from collections import defaultdict

def mapping_key(dbms, columns):
    grouped_data = defaultdict(list)  
    final_mapping = {}
    for col in columns:
        # tofix: bin_1(didn't appear)
        match = re.match(r'(.+)_(\d+)$', col)
        if match:
            prefix, y = match.groups()
            grouped_data[prefix].append((int(y), col))  
        else:
            final_mapping[col] = col 
                
    for key in sorted(grouped_data.keys()): 
        grouped_data[key].sort() 
        for i, (_, original) in enumerate(grouped_data[key]):
            if (dbms != "postgresql"):
                final_mapping[original] = f'{key}[{i+1}]' 
            else:
                final_mapping[original] = f'"{key}"[{i+1}]'
                
    return final_mapping
    