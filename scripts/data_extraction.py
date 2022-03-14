import io
import re
import pandas as pd
from pathlib import Path
from itertools import chain
from pandas import Timestamp as ts
from openpyxl import load_workbook
from datetime import datetime as dt
from openpyxl.utils.cell import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

from ra_logging import TextLogger
from configuration_options import Paths,Organizations

def open_workbook(path:Path,data_only:bool=True,read_only:bool=True,in_mem:bool=True,logger:TextLogger=TextLogger()):
    '''
    loads an excel workbook based on a given dictionary of paths matched to a set of parameters
    parameters:
        path - a Path object which points to an excel xlsx file
        data_only - openpyxl parameter to open workbook in data-only mode
            (i.e., evaluated values instead of formulas)
        read_only - openpyxl paramater to open workbook in read-only mode,
            which can improve load times
        in_mem - if true, loads workbook into python-managed memory rather of
            reading from disc, which can improve random access read times
    '''
    if path is not None:
        if path.is_file() and re.match(r'^.*\.xlsx$',path.name):
            if in_mem:
                with path.open('rb') as f:
                    in_mem_file = io.BytesIO(f.read())
                    workbook = load_workbook(in_mem_file,data_only=data_only,read_only=read_only)
            else:
                workbook = load_workbook(str(path),data_only=data_only,read_only=read_only)
            logger.log('Loaded Workbook {}'.format(path.name),'INFORMATION')
        else:
            workbook = None
            logger.log('File Not Found: {}'.format(path.name),'WARNING')
    else:
        workbook = None
        logger.log('Unable to Load File: {}'.format(str(path)),'WARNING')
    return workbook

def get_data_range(worksheet:Worksheet,lse_column:str,data_columns:str,organizations:Organizations):
    '''
    extracts a table from a worksheet with rows corresponding to each load-serving entity
    parameters:
        worksheet - an excel worksheet expected to contain the data table
        lse_column - the column letter identifying the column in which load-
            serving entities are expected to be listed
        data_columns - a list of letters identifying columns from which to
            retrieve data matched to the load-serving entities identified in
            the lse_column
        organizations - an instance of the Organizations class containing
            information about each load-serving entity
    '''
    first_row = 0
    last_row = 0
    organization_ids = [organization['id'].lower() for organization in organizations.list_load_serving_entities()]
    for row_number in range(1,worksheet.max_row+1):
        if first_row==0 and str(worksheet['{}{}'.format(lse_column,row_number)].value).lower() in organization_ids:
            first_row = row_number
        if first_row>0 and last_row==0 and str(worksheet['{}{}'.format(lse_column,row_number+1)].value).lower() in ('none','total','total:','total cpuc juris'):
            last_row = row_number
            break
    if first_row>0 and last_row>0:
        # return a list of lists containing excel cell objects:
        data_range = []
        for row_number in range(first_row,last_row+1):
            data_range.append([worksheet['{}{}'.format(lse_column,row_number)]]+[worksheet['{}{}'.format(data_column,row_number)] for data_column in data_columns])
        return data_range
    else:
        # return a list containing an empty list;
        return [[]]
    
def data_range_to_dataframe(columns,data_range):
    '''
    converts a excel data range into a pandas dataframe
    parameters:
        columns - list of column labels to apply to the dataframe
        data_range - excel data range containing cells to insert into new
            dataframe
    '''
    data_array = []
    for data_range_row in data_range:
        data_array_row = []
        for data_range_cell in data_range_row:
            data_array_row.append(data_range_cell.value)
        data_array.append(data_array_row)
    return pd.DataFrame(data_array,columns=columns)

# find and return data range for the flex requirements net cam table:
def get_table(worksheet,table_header_text:str,table_header_offset:dict,columns:list):
    '''
    searches a given worksheet for table header text and returns a dataframe
    based on a table with an upper left corner determined by a header offset, a
    height defined by consecutive non-empty cells in the first column, and a
    width defined by the number of column labels.
    parameters:
        worksheet - an excel worksheet expected to contain the table header
        table_header_text - a string which, if found identically within the
            worksheet, will be used as the reference coordinates for the table
        
    '''
    table_bounds = {
        'top' : worksheet.max_row,
        'bottom' : worksheet.max_row,
        'left' : get_column_letter(worksheet.max_column),
        'right' : get_column_letter(worksheet.max_column),
    }
    for row_index,row in enumerate(worksheet.rows):
        if row_index+1<table_bounds['bottom']:
            for column_index,cell in enumerate(row):
                if str(cell.value).lower()==table_header_text.lower():
                    # found header, setting upper, left, and right boundaries of table (row and column indices are excel label-1):
                    table_bounds['top'] = row_index + 1 + table_header_offset['rows']
                    table_bounds['left'] = get_column_letter(column_index + table_header_offset['columns'] + 1)
                    table_bounds['right'] = get_column_letter(column_index + table_header_offset['columns'] + len(columns))
                if row_index+1>table_bounds['top'] and get_column_letter(column_index+1)==table_bounds['left'] and str(cell.value).replace(' ','') in ('None',''):
                    # found last row in table:
                    table_bounds['bottom'] = row_index
                    break
        else:
            break
    data_range = worksheet['{left}{top}:{right}{bottom}'.format(**table_bounds)]
    return data_range_to_dataframe(columns,data_range)

def read_ra_monthly_filing(organization:dict,paths:Paths,logger:TextLogger,date:ts=None,version:int=None):
    '''
    extracts relevant data tables from a monthly filing workbook for a given
    lse and month
    parameters:
        organization - the abbreviated identifier string for an active load-
            serving entity
        paths - dictionary providing paths to filings for each load-serving
            entity and month
        logger - text_logger object for logging errors and other information
        date - an option datetime object representing the filing month; if left
            blank, the filing month defined in the paths object will be applied
        version - an optional integer version number; if left blank, the
            highest numbered version of a filing for the given load-serving
            entity and month will be used
    '''
    physical_resources_columns=[
        'organization_id',
        'contract_id',
        'resource_id',
        'resource_adequacy_system',
        'resource_adequacy_local',
        'resource_mcc_bucket',
        'continuous_availability',
        'resource_adequacy_committed_flexible',
        'resource_adequacy_flexibility_category',
        'start_date',
        'end_date',
        'scid',
        'zone',
        'local_area',
    ]
    demand_response_columns = [
        'organization_id',
        'contract_id',
        'program_id',
        'resource_adequacy_system',
        'resource_adequacy_local',
        'resource_mcc_bucket',
        'third_party_program',
        'resource_adequacy_committed_flexible',
        'resource_adequacy_flexibility_category',
    ]
    path = paths.get_path('ra_monthly_filing',organization=organization,date=date,version=version)
    ra_monthly_filing = open_workbook(path,in_mem=False,logger=logger)
    if ra_monthly_filing is not None:
        logger.log('Consolidating Filing Data for {name} ({id})'.format(**organization),'INFORMATION')
        sheet_names = [
            'Certification',
            'LSE Allocations',
            'ID and Local Area',
            'Summary Year Ahead',
            'Summary Month Ahead',
            'I_Phys_Res_Import_RA_Res',
            'II_Construc','III_Demand_Response'
        ]
        sheet_check = all([sheet_name in ra_monthly_filing.sheetnames for sheet_name in sheet_names])
        if sheet_check:
            # check that each required column is present:
            column_checks = [
                r'.*contract\s+identifier.*',
                r'.*resource\s+id.*',
                r'.*system\s+ra.*',
                r'.*local\s+ra.*',
                r'.*mcc.*',
                r'.*available.*',
                r'.*flexible\s+ra.*',
                r'.*flexible\s+category.*',
                r'.*start\s+date.*',
                r'.*end\s+date.*',
                r'.*scid.*',
                r'.*zonal.*',
                r'.*area.*',
                r'north',
                r'south',
            ]
            cell_values = [cell.value for cell in ra_monthly_filing['I_Phys_Res_Import_RA_Res']['B3:P3'][0]]
            phys_res_check = all([re.match(column_check,str(cell_values[i]).lower()) for i,column_check in enumerate(column_checks)])
            column_checks = [
                r'.*identifier.*',
                r'.*program.*',
                r'.*system.*',
                r'.*local.*',
                r'.*mcc.*',
                r'.*third\s+party.*',
                r'.*flexible\s+ra.*',
                r'.*flexible\s+category.*',
                r'.*start\s+date.*',
                r'.*end\s+date.*',
                r'.*operator.*',
                r'.*zonal.*',
                r'.*area.*',
                r'.*do\s+not\s+delete.*',
                r'north',
                r'south',
                r'pg.*e',
                r'sce',
                r'sdge',
            ]
            worksheet = ra_monthly_filing['III_Demand_Response']
            cell_values = [cell.value for cell in chain(worksheet['B3:O3'][0],worksheet['O4:P4'][0],[worksheet['R3'],worksheet['T3'],worksheet['V3']])]
            demand_response_check = all([re.match(column_check,str(cell_values[i]).lower()) for i,column_check in enumerate(column_checks)])
        else:
            phys_res_check = False
            demand_response_check = False
        if all([sheet_check,phys_res_check,demand_response_check]):
            # retrieve values for summary table:
            summary = pd.DataFrame({
                'organization_id' : [organization['id']],
                'organization_officer_name' : [ra_monthly_filing['Certification']['B21'].value],
                'organization_officer_title' : [ra_monthly_filing['Certification']['B22'].value],
                'np26dr' : [
                        ra_monthly_filing['III_Demand_Response']['O5'].value +
                        ra_monthly_filing['III_Demand_Response']['S13'].value * 0.097 / 1.097
                    ],
                'sp26dr' : [
                        ra_monthly_filing['III_Demand_Response']['P5'].value +
                        ra_monthly_filing['III_Demand_Response']['U8'].value * 0.076 / 1.076 +
                        ra_monthly_filing['III_Demand_Response']['W6'].value * 0.096 / 1.096
                    ],
            })
            # helper function to find last row of physical resources tables:
            def get_last_row(sheet):
                last_row = sheet.max_row
                for row_index,row in enumerate(sheet.rows):
                    if row_index>5 and str(row[3].value).replace(' ','') in ('','None'):
                        last_row = row_index
                        break
                    else:
                        pass
                return last_row

            # retrieve physical resources table:
            last_row = get_last_row(ra_monthly_filing['I_Phys_Res_Import_RA_Res'])
            if last_row > 5:
                physical_resources = data_range_to_dataframe(physical_resources_columns,ra_monthly_filing['I_Phys_Res_Import_RA_Res']['A5:N{}'.format(last_row)])
            else:
                physical_resources = pd.DataFrame(columns=physical_resources_columns)
            physical_resources.loc[:,'organization_id'] = organization['id']
            physical_resources.replace('',0,inplace=True)
            physical_resources.loc[:,'continuous_availability'] = physical_resources.loc[:,'continuous_availability'].map(lambda s: True if s=='Y' else False)

            # retrieve demand response table:
            last_row = get_last_row(ra_monthly_filing['III_Demand_Response'])
            if last_row > 17:
                demand_response = data_range_to_dataframe(demand_response_columns,ra_monthly_filing['III_Demand_Response']['A17:I{}'.format(last_row)])
            else:
                demand_response = pd.DataFrame(columns=demand_response_columns)
            demand_response.loc[:,'organization_id'] = organization['id']
            demand_response.loc[:,'third_party_program'] = demand_response.loc[:,'third_party_program'].map(lambda s: True if s=='Y' else False)

        else:
            logger.log('Filing Did Not Pass Validation: {name} ({id})'.format(**organization),'WARNING')
            summary = pd.DataFrame({
                'organization_id' : [organization['id']],
                'organization_officer_name' : ['[Filing Did Not Pass Validation]'],
                'organization_officer_title' : ['[N/A]'],
                'np26dr' : [0],
                'sp26dr' : [0],
            })
            physical_resources = pd.DataFrame(columns=physical_resources_columns)
            demand_response = pd.DataFrame(columns=demand_response_columns)

        # close monthly filing workbook:
        ra_monthly_filing.close()

    # return void dataframe rows when monthly filing for lse is not found:
    else:
        logger.log('Unable to Find Filing Data for {name} ({id})'.format(**organization),'WARNING')
        summary = pd.DataFrame({
            'organization_id' : [organization['id']],
            'organization_officer_name' : ['[Monthly Filing Not Found]'],
            'organization_officer_title' : ['[N/A]'],
            'np26dr' : [0],
            'sp26dr' : [0],
        })
        physical_resources = pd.DataFrame(columns=physical_resources_columns)
        demand_response = pd.DataFrame(columns=demand_response_columns)
    return {
        'summary'  : summary,
        'physical_resources' : physical_resources,
        'demand_response' : demand_response,
    }

def get_year_ahead_tables(year_ahead,filing_month,organizations):
    '''
    loads relevant data from the year-ahead workbook into dataframes
    parameters:
        year_ahead - year-ahead workbook
    '''
    # get load forecast input table from the year ahead workbook:
    columns = [
        'iou_territory',
        'month',
        'organization_id',
        'lse_type',
        'submitted_forecast',
        'coincidence_adjustment',
        'coincident_peak_forecast',
        'lse_specific_total',
        'copkadj_with_lseadj',
        'eelmdr_adjustment',
        'adjusted_with_lmdr',
        'pro_rata_adjustment',
        'final_coincident_peak_forecast',
    ]
    data_range = year_ahead['loadforecastinputdata']['B2:N{}'.format(year_ahead['loadforecastinputdata'].max_row)]
    load_forecast_input_data = data_range_to_dataframe(columns,data_range)
    load_forecast_input_data.dropna(axis='index',how='all',inplace=True)
    load_forecast_input_data.loc[:,'month'] = load_forecast_input_data.loc[:,'month'].map(lambda s: ts(filing_month.year,int(s),1)).astype('datetime64[M]')
    load_forecast_input_data.set_index(['iou_territory','organization_id','month'],inplace=True)
    load_forecast_input_data.sort_index(inplace=True)
    # accommodate errant entries:
    load_forecast_input_data.loc[:,columns[4:]] = load_forecast_input_data.loc[:,columns[4:]].applymap(lambda x: 0 if isinstance(x,str) else x)

    # demand response allocation table:
    month_columns = [ts(filing_month.year,month,1) for month in range(1,13)]
    columns = [
        'location',
    ] + month_columns
    data_range = year_ahead['DRforAllocation']['D2:P32']
    demand_response_allocation = data_range_to_dataframe(columns,data_range)
    demand_response_allocation['location'] = demand_response_allocation.loc[:,'location'].map(lambda x: str(x).lower().replace(' ','_'))
    demand_response_allocation = demand_response_allocation.melt(id_vars=['location'],var_name='month',value_name='allocation')
    demand_response_allocation.loc[:,'month'] = demand_response_allocation.loc[:,'month'].astype('datetime64[M]')
    demand_response_allocation.set_index(['location','month'],inplace=True)
    demand_response_allocation.sort_index(inplace=True)

    # cam credits:
    columns = [
        'iou_territory',
        'category',
    ] + month_columns
    data_range = year_ahead['Flexrequirements']['R4:AE12']
    cam_credits = data_range_to_dataframe(columns,data_range)
    cam_credits = cam_credits.melt(id_vars=['iou_territory','category'],var_name='month',value_name='cam_credit')
    cam_credits.set_index(['iou_territory','category','month'],inplace=True)
    cam_credits.sort_index(inplace=True)

    # flexibility requirements:
    table_header_text = 'Flex Requirements net CAM'
    table_header_offset = {
        'rows' : 1,
        'columns' : -1,
    }
    month_columns = [ts(filing_month.year,month,1).to_numpy().astype('datetime64[M]') for month in range(1,13)]
    columns = [
        'organization_id',
        'flex_category',
    ] + month_columns
    flexibility_requirements = get_table(year_ahead['Flexrequirements'],table_header_text,table_header_offset,columns)
    flexibility_requirements['flex_category'] = flexibility_requirements.loc[:,'flex_category'].map(lambda s: int(s[-1]))
    flexibility_requirements = flexibility_requirements.melt(id_vars=['organization_id','flex_category'],var_name='month',value_name='flexibility_requirement')
    flexibility_requirements.loc[:,'month'] = flexibility_requirements.loc[:,'month'].astype('datetime64[M]')
    flexibility_requirements.set_index(['organization_id','flex_category','month'],inplace=True)
    flexibility_requirements.sort_index(inplace=True)

    # flex-rmr:
    columns = ['organization_id'] + month_columns
    data_range = get_data_range(year_ahead['Flex RMR'],'A','BCDEFGHIJKLM',organizations)
    flexibility_rmr = data_range_to_dataframe(columns,data_range)
    flexibility_rmr = flexibility_rmr.melt(id_vars=['organization_id'],var_name='month',value_name='flexibility_rmr')
    flexibility_rmr.set_index(['organization_id','month'],inplace=True)
    flexibility_rmr.sort_index(inplace=True)

    # local cam:
    columns = [
        'organization_id',
        'los_angeles',
        'ventura',
        'san_diego',
        'bay_area',
        'fresno',
        'sierra',
        'stockton',
        'kern',
        'humboldt',
        'northern_california',
    ]
    data_range = get_data_range(year_ahead['Local RA-CAM-{}'.format(filing_month.year)],'B','CDEFGHIJKL',organizations)
    local_rar = data_range_to_dataframe(columns,data_range)
    local_rar.set_index('organization_id',inplace=True)
    local_rar.sort_index(inplace=True)
    local_rar.fillna(value=0,inplace=True)

    # total lcr:
    data_range = year_ahead['Local RA-CAM-{}'.format(filing_month.year)]['C2:L2']
    columns = [cell.value for cell in data_range[0]]
    data_range = year_ahead['Local RA-CAM-{}'.format(filing_month.year)]['C4:L4']
    total_lcr = data_range_to_dataframe(columns,data_range)
    return [load_forecast_input_data,demand_response_allocation,cam_credits,flexibility_requirements,flexibility_rmr,local_rar,total_lcr]
    
def get_incremental_local_tables(incremental_local,organizations:Organizations):
    '''
    loads relevant data from the incremental local year-ahead adjustment
    workbook into dataframes
    parameters:
        incremental_local - incremental local year-ahead workbook
        organizations - an instance of the Organizations class
    '''
    columns = [
        'organization_id',
        1,
        2,
        3,
    ]
    data_range = get_data_range(incremental_local['IncrementalLocal'],'A','MNO',organizations)
    incremental_flex = data_range_to_dataframe(columns,data_range)
    incremental_flex = incremental_flex.melt(id_vars=['organization_id'],var_name='category',value_name='flexibility_requirement')
    incremental_flex.set_index(['organization_id','category'],inplace=True)
    incremental_flex.sort_index(inplace=True)
    columns = [
        'organization_id',
        'los_angeles',
        'ventura',
        'san_diego',
        'bay_area',
        'fresno',
        'sierra',
        'stockton',
        'kern',
        'humboldt',
        'northern_california',
    ]
    data_range = get_data_range(incremental_local['IncrementalLocal'],'A','BCDEFGHIJK',organizations)
    incremental_local_load = data_range_to_dataframe(columns,data_range)
    incremental_local_load = incremental_local_load.melt(id_vars=['organization_id'],var_name='location',value_name='incremental_load')
    incremental_local_load.set_index(['organization_id','location'],inplace=True)
    incremental_local_load.sort_index()
    columns = [
        'organization_id',
        'los_angeles',
        'ventura',
        'san_diego',
        'bay_area',
        'fresno',
        'sierra',
        'stockton',
        'kern',
        'humboldt',
        'northern_california',
        'sp26_cam_capacity',
        'sp26_condition2_rmr',
        'np26_condition2_rmr',
        'path26_ns',
        'path26_sn',
        'np26_cam_capacity',
    ]
    local_rar_trueup = get_table(incremental_local['Local Trueup'],'YA Local RAR Allocations',{'rows':6,'columns':1},columns)
    local_rar_trueup = local_rar_trueup.melt(id_vars=['organization_id'],var_name='location',value_name='local_rar_trueup')
    local_rar_trueup.set_index(['organization_id','location'],inplace=True)
    local_rar_trueup.sort_index(inplace=True)
    return [incremental_flex,incremental_local_load,local_rar_trueup]
    
def get_month_ahead_forecasts(month_ahead):
    '''
    loads relevant data from the month-ahead forecasts workbook into dataframes
    parameters:
        month_ahead - month-ahead workbook
    '''
    columns = [
        'organization_id',
        'lse_type',
        'jurisdiction',
        'lse_lu',
        'month',
        'id_and_date',
        'sce_year_ahead_forecast',
        'sdge_year_ahead_forecast',
        'pge_year_ahead_forecast',
        'total_year_ahead_forecast',
        'sce_esps_migrating_load',
        'sce_cca_migrating_load',
        'sdge_esps_migrating_load',
        'sdge_cca_migrating_load',
        'pge_esps_migrating_load',
        'pge_cca_migrating_load',
        'sce_migration_adjustment',
        'sdge_migration_adjustment',
        'pge_migration_adjustment',
        'total_migration_adjustment',
        'sce_revised_monthly_forecast',
        'sdge_revised_monthly_forecast',
        'pge_revised_monthly_forecast',
        'total_revised_monthly_forecast',
        'sce_revised_noncoincident_monthly_forecast',
        'sdge_revised_noncoincident_monthly_forecast',
        'pge_revised_noncoincident_monthly_forecast',
        'total_revised_noncoincident_monthly_forecast',
        'sce_revised_nonjurisdictional_load_share',
        'sdge_revised_nonjurisdictional_load_share',
        'pge_revised_nonjurisdictional_load_share',
        'total_revised_nonjurisdictional_load_share',
        'sce_revised_jurisdictional_load_share',
        'sdge_revised_jurisdictional_load_share',
        'pge_revised_jurisdictional_load_share',
        'total_revised_jurisdictional_load_share',
    ]
    data_range = month_ahead['Monthly Tracking for CPUC']['B5:AK{}'.format(month_ahead['Monthly Tracking for CPUC'].max_row)]
    month_ahead_forecasts = data_range_to_dataframe(columns,data_range)
    month_ahead_forecasts['organization_id'] = month_ahead_forecasts.loc[:,'organization_id'].map(lambda s: s.replace('Total','').strip() if isinstance(s,str) else s)
    month_ahead_forecasts.set_index(['organization_id','month'],inplace=True)
    month_ahead_forecasts.sort_index(inplace=True)
    return month_ahead_forecasts

def get_cam_rmr_tables(cam_rmr):
    '''
    loads relevant data from the cam-rmr workbook into dataframes
    parameters:
        cam_rmr - cam-rmr workbook
    '''
    columns = [
        'organization_id',
        'lse_type',
        'jurisdiction',
        'lse_lu',
        'month',
        'id_and_date',
        'sce_year_ahead_forecast',
        'sdge_year_ahead_forecast',
        'pge_year_ahead_forecast',
        'total_year_ahead_forecast',
        'sce_esps_migrating_load',
        'sce_cca_migrating_load',
        'sdge_esps_migrating_load',
        'sdge_cca_migrating_load',
        'pge_esps_migrating_load',
        'pge_cca_migrating_load',
        'sce_load_migration_adjustment',
        'sdge_load_migration_adjustment',
        'pge_load_migration_adjustment',
        'total_load_migration_adjustment',
        'sce_revised_monthly_forecast',
        'sdge_revised_monthly_forecast',
        'pge_revised_monthly_forecast',
        'total_revised_monthly_forecast',
        'sce_revised_noncoincident_monthly_forecast',
        'sdge_revised_noncoincident_monthly_forecast',
        'pge_revised_noncoincident_monthly_forecast',
        'total_revised_noncoincident_monthly_forecast',
        'sce_revised_nonjurisdictional_load_share',
        'sdge_revised_nonjurisdictional_load_share',
        'pge_revised_nonjurisdictional_load_share',
        'total_revised_nonjurisdictional_load_share',
        'blank',
        'sce_revised_jurisdictional_load_share',
        'sdge_revised_jurisdictional_load_share',
        'pge_revised_jurisdictional_load_share',
        'total_revised_jurisdictional_load_share',
    ]
    data_range = cam_rmr['monthlytracking']['B5:AL{}'.format(cam_rmr['monthlytracking'].max_row)]
    cam_rmr_monthly_tracking = data_range_to_dataframe(columns,data_range)
    cam_rmr_monthly_tracking.drop('blank',axis='columns',inplace=True)
    cam_rmr_monthly_tracking.dropna(subset=['organization_id','month'],inplace=True)
    cam_rmr_monthly_tracking['organization_id'] = cam_rmr_monthly_tracking.loc[:,'organization_id'].map(lambda s: s.replace('Total','').strip() if isinstance(s,str) else s)
    cam_rmr_monthly_tracking.set_index(['organization_id','month'],inplace=True)
    cam_rmr_monthly_tracking.sort_index(inplace=True)
    columns = [
        'np26_cam',
        'sp26_cam',
        'np26_rmr',
        'sp26_rmr',
        'system_rmr',
        'sce_preferred_lcr_credit',
        'sdge_cam',
        'sce_cam',
    ]
    data_range = cam_rmr['CAMRMR']['B4:I4']
    total_cam_rmr = pd.Series(data=[cell.value for cell in data_range[0]],index=columns)

    return [cam_rmr_monthly_tracking,total_cam_rmr]

def get_summary_tables(ra_summary):
    '''
    loads select data from a summary workbook into dataframes
    parameters:
        ra_summary - resource adequacy monthly summary workbook
    '''
    columns = [
        'organization_id',
        'resource_adequacy_obligation',
        'physical_resources',
        'demand_response_resources',
        'total_resources',
        'percent_obligation_available',
    ]
    data_range = get_data_range(ra_summary['Summary'],'A','BCDEF')
    summary = data_range_to_dataframe(columns,data_range)
    summary.set_index('organization_id',inplace=True)
    columns = [
        'organization_id',
        'resource_adequacy_obligation',
        'total_flex_capacity',
        'percent_obligation_available',
        'category_1_rar',
        'category_1_countable',
        'category_2_rar',
        'category_2_countable',
        'category_3_rar',
        'category_3_countable',
        'category_1_flex',
        'category_2_flex',
        'category_3_flex',
        'total_flex',
    ]
    data_range = get_data_range(ra_summary['FlexRAR'],'A','BCDEFGHIJLMNO')
    flex_rar = data_range_to_dataframe(columns,data_range)
    flex_rar.set_index('organization_id',inplace=True)
    columns = [
        'organization_id',
        'los_angeles_rar','los_angeles_incremental_adjustment','los_angeles_demand_response_allocation','los_angeles_procurement','los_angeles_compliance',
        'ventura_rar','ventura_incremental_adjustment','ventura_demand_response_allocation','ventura_procurement','ventura_compliance',
        'san_diego_rar','san_diego_incremental_adjustment','san_diego_demand_response_allocation','san_diego_procurement','san_diego_compliance',
        'bay_area_rar','bay_area_incremental_adjustment','bay_area_demand_response_allocation','bay_area_procurement','bay_area_compliance',
        'humboldt_rar','humboldt_incremental_adjustment','humboldt_demand_response_allocation','humboldt_procurement','humboldt_compliance',
        'sierra_rar','sierra_incremental_adjustment','sierra_demand_response_allocation','sierra_procurement','sierra_compliance',
        'stockton_rar','stockton_incremental_adjustment','stockton_demand_response_allocation','stockton_procurement','stockton_compliance',
        'northern_california_rar','northern_california_incremental_adjustment','northern_california_demand_response_allocation','northern_california_procurement','northern_california_compliance',
        'fresno_rar','fresno_incremental_adjustment','fresno_demand_response_allocation','fresno_procurement','fresno_compliance',
        'kern_rar','kern_incremental_adjustment','kern_demand_response_allocation','kern_procurement','kern_compliance',
        'pge_other_rar','pge_other_incremental_adjustment','pge_other_demand_response_allocation','pge_other_procurement','pge_other_compliance',
        'sce_tac_other_rar','sce_tac_other_incremental_adjustment','sce_tac_other_demand_response_allocation','sce_tac_other_procurement','sce_tac_other_compliance',
        'sdge_tac_other_rar','sdge_tac_other_incremental_adjustment','sdge_tac_other_demand_response_allocation','sdge_tac_other_procurement','sdge_tac_other_compliance',
        'pge_tac_other_rar','pge_tac_other_incremental_adjustment','pge_tac_other_demand_response_allocation','pge_tac_other_procurement','pge_tac_other_compliance',
    ]
    data_range = get_data_range(ra_summary['LocalTrueUp'],'A',list(map(get_column_letter,range(2,72))))
    local_trueup = data_range_to_dataframe(columns,data_range)
    local_trueup.set_index('organization_id',inplace=True)

    return [summary,flex_rar,local_trueup]

def get_nqc_list(ra_summary,filing_month):
    '''
    retrieves the monthly generation capacities from a given resource adequacy summary sheet
    parameters:
        ra_summary - a workbook containing the monthly resource adequacy summary data
        filing_month - month of resource adequacy filings
    '''
    month_columns = [ts(filing_month.year,month,1).to_numpy().astype('datetime64[M]') for month in range(1,13)]
    columns = [
        'generator_name',
        'resource_id',
        'zone',
        'local_area',
    ] + month_columns + [
        'dispatchable',
        'deliverability_status',
        'deliverable',
        'comments',
    ]
    data_range = ra_summary['NQC_List']['A2:T{}'.format(ra_summary['NQC_List'].max_row)]
    nqc_list = data_range_to_dataframe(columns,data_range)
    return nqc_list