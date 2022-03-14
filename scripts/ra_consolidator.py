import xlrd
import numpy as np
import pandas as pd
from pathlib import Path
from pandas import Timestamp as ts
from openpyxl.styles import PatternFill,Font
from openpyxl.formatting.rule import CellIsRule
from openpyxl.utils.cell import get_column_letter

from ra_logging import TextLogger,AttachmentLogger,ConsolidationLogger
from configuration_options import ConfigurationOptions,Paths,Organizations
from data_extraction import open_workbook,get_data_range,data_range_to_dataframe,read_ra_monthly_filing,get_year_ahead_tables,get_month_ahead_forecasts,get_cam_rmr_tables,get_incremental_local_tables,get_nqc_list

class WorkbookConsolidator:
    '''
    this class contains methods to collect data from allocations, filings, and
    supply plans into monthly summary and cross-check files.
    '''
    def __init__(self,configuration_path:Path):
        '''
        initializes an instance of the ra_consolidator class
        parameters:
            configuration_path - path object pointing to a yaml file containing
                configuration options for the ra_consolidator object
        '''
        self.configuration_options = ConfigurationOptions(configuration_path)
        self.paths = Paths(self.configuration_options)
        self.organizations = Organizations(self.paths.get_path('organizations'))
        self.logger = TextLogger(
            self.configuration_options.get_option('cli_logging_criticalities'),
            self.configuration_options.get_option('file_logging_criticalities'),
            self.paths.get_path('log')
        )
        self.attachment_logger = AttachmentLogger(self.paths.get_path('attachment_log'))
        self.consolidation_logger = ConsolidationLogger(self.paths.get_path('consolidation_log'))

    # copy summary template to new summary file:
    def initialize_ra_summary(self):
        self.logger.log('Creating New Monthly RA Summary File from Template: {}'.format(self.paths.get_path('ra_summary_template').name),'INFORMATION')

        # open ra_summary_starter file into memory:
        try:
            path = self.paths.get_path('ra_summary_template')
            ra_summary = open_workbook(path,data_only=False,read_only=False,in_mem=True)
            ra_summary.save(str(self.paths.get_path('ra_summary')))
            ra_summary.close()
            self.logger.log('Created New Monthly RA Summary File: {}'.format(self.paths.get_path('ra_summary').name),'INFORMATION')
        except:
            self.logger.log('Unable to Create New Monthly RA Summary File','ERROR')

    # copy caiso supply plan cross-check template to new monthly cross-check file:
    def initialize_caiso_cross_check(self):
        self.logger.log('Creating New CAISO Supply Plan Cross-Check File from Template: {}'.format(self.paths.get_path('caiso_cross_check_template').name),'INFORMATION')
        try:
            path = self.paths.get_path('caiso_cross_check_template')
            caiso_cross_check = open_workbook(path,data_only=False,read_only=False,in_mem=True)
            caiso_cross_check.save(str(self.paths.get_path('caiso_cross_check')))
            caiso_cross_check.close()
            self.logger.log('Created New CAISO Supply Plan Cross-Check File: {}'.format(self.paths.get_path('caiso_cross_check').name),'INFORMATION')
        except:
            self.logger.log('Unable to Create New CAISO Supply Plan Cross-Check File','ERROR')

    # open each applicable file and copy data into summary sheet:
    def consolidate_allocations(self):
        self.logger.log('Consolidating Allocation Data','INFORMATION')

        # start timer:
        init_time = ts.now()

        filing_month = self.configuration_options.get_option('filing_month')

        # get source data from year ahead file:
        path = self.paths.get_path('year_ahead')
        year_ahead = open_workbook(path)
        year_ahead_tables = get_year_ahead_tables(year_ahead,filing_month,self.organizations)
        load_forecast_input_data = year_ahead_tables[0]
        demand_response_allocation = year_ahead_tables[1]
        flexibility_requirements = year_ahead_tables[3]
        flexibility_rmr = year_ahead_tables[4]
        local_rar = year_ahead_tables[5]
        year_ahead.close()

        # get source data from month ahead file:
        path = self.paths.get_path('month_ahead')
        month_ahead = open_workbook(path,in_mem=False)
        month_ahead_forecasts = get_month_ahead_forecasts(month_ahead)
        month_ahead.close()

        # get source data from cam-rmr file:
        path = self.paths.get_path('cam_rmr')
        cam_rmr = open_workbook(path,in_mem=False)
        [cam_rmr_monthly_tracking,total_cam_rmr] = get_cam_rmr_tables(cam_rmr)
        cam_rmr.close()

        # get source data from incremental local workbook:
        if filing_month.month >= 6:
            path = self.paths.get_path('incremental_local')
            incremental_local = open_workbook(path)
            [incremental_flex,incremental_local_load,local_rar_trueup] = get_incremental_local_tables(incremental_local,self.organizations)
            incremental_local.close()
        else:
            [incremental_flex,incremental_local_load,local_rar_trueup] = [None,None,None]

        # open summary file and initialize summary table:
        path = self.paths.get_path('ra_summary')
        ra_summary = open_workbook(path,data_only=False,read_only=False)
        columns = [
            'organization_id',
        ]
        data_range = get_data_range(ra_summary['Summary'],'A','',self.organizations)
        summary = data_range_to_dataframe(columns,data_range)

        # open caiso supply plan cross-check file:
        path = self.paths.get_path('caiso_cross_check')
        caiso_cross_check = open_workbook(path,data_only=False,read_only=False)

        # create function to use in dataframe.apply():
        def calculate_summary(row):
            organization_id = row.loc['organization_id']
            inverse_organization_selection = list(dict.fromkeys(filter(lambda idx: idx!=organization_id,cam_rmr_monthly_tracking.index.get_level_values(0))).keys())
            # NP26 summary sheet:
            # if filing_month.month>=9 and organization_id=='PGE':
            if organization_id=='PGE':
                cam_load_share = -cam_rmr_monthly_tracking.loc[(inverse_organization_selection,filing_month),'pge_revised_nonjurisdictional_load_share'].sum()
            else:
                cam_load_share = cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'pge_revised_nonjurisdictional_load_share']
            np26_ra_obligation = np.round(
                (
                    # PGEload:
                    (1 + self.configuration_options.get_option('planning_reserve_margin')) * month_ahead_forecasts.loc[(organization_id,filing_month),'pge_revised_monthly_forecast']
                    # NP26CAM:
                    -total_cam_rmr.loc['np26_cam'] * cam_load_share
                    # NP26RMR:
                    -total_cam_rmr.loc['np26_rmr'] * cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'pge_revised_nonjurisdictional_load_share']
                ),
                0
            ) - total_cam_rmr.loc['system_rmr'] * cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'total_revised_jurisdictional_load_share']
            sn_path26_allocation = 0
            # SP26 summary sheet:
            # if filing_month.month>=9 and organization_id=='SCE':
            if organization_id=='SCE':
                cam_load_share_sce = -cam_rmr_monthly_tracking.loc[(inverse_organization_selection,filing_month),'sce_revised_nonjurisdictional_load_share'].sum()
                cam_load_share_sdge = cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'sdge_revised_nonjurisdictional_load_share']
            # elif filing_month.month>=9 and organization_id=='SDGE':
            elif organization_id=='SDGE':
                cam_load_share_sce = cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'sce_revised_nonjurisdictional_load_share']
                cam_load_share_sdge = -cam_rmr_monthly_tracking.loc[(inverse_organization_selection,filing_month),'sdge_revised_nonjurisdictional_load_share'].sum()
            else:
                cam_load_share_sce = cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'sce_revised_nonjurisdictional_load_share']
                cam_load_share_sdge = cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'sdge_revised_nonjurisdictional_load_share']
            sp26_ra_obligation = np.round(
                (
                    (1 + self.configuration_options.get_option('planning_reserve_margin')) * (
                        # SCEload:
                        month_ahead_forecasts.loc[(organization_id,filing_month),'sce_revised_monthly_forecast']
                        # SDGEload:
                        +month_ahead_forecasts.loc[(organization_id,filing_month),'sdge_revised_monthly_forecast']
                    )
                    # SP26CAM:
                    -total_cam_rmr.loc['sce_cam'] * cam_load_share_sce
                    -total_cam_rmr.loc['sdge_cam'] * cam_load_share_sdge
                    # SP26RMR:
                    -total_cam_rmr.loc['sp26_rmr'] * cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'sce_revised_nonjurisdictional_load_share']
                    #-total_cam_rmr.loc['sp26_rmr'] * cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'sdge_revised_nonjurisdictional_load_share']
                    # SCELCR:
                    -total_cam_rmr.loc['sce_preferred_lcr_credit'] * cam_rmr_monthly_tracking.loc[(organization_id,filing_month),'sce_revised_jurisdictional_load_share']
                ),
                0
            )
            ns_path26_allocation = 0
            def incremental_flex_by_category(category: int):
                if incremental_flex:
                    flex = incremental_flex.loc[(organization_id,category),'flexibility_requirement']
                else:
                    flex = 0
                return flex
            def incremental_load_by_area(area: str):
                if incremental_local_load:
                    incremental_load = incremental_local_load.loc[(organization_id,area),'incremental_load']
                else:
                    incremental_load = 0
                return incremental_load
            def august_demand_response(iou_territory: str,location: str):
                month = ts(filing_month.year,8,1)
                if (iou_territory,organization_id,month) in load_forecast_input_data.index and (location,month) in demand_response_allocation.index:
                    august_forecast_lse = load_forecast_input_data.loc[(iou_territory,organization_id,month),'final_coincident_peak_forecast']
                    organization_id_indices = list(dict.fromkeys(load_forecast_input_data.loc[(iou_territory),:].index.get_level_values(0)))
                    august_forecast_local = load_forecast_input_data.loc[(iou_territory,organization_id_indices,month),'final_coincident_peak_forecast'].sum()
                    august_demand_response_allocation = demand_response_allocation.loc[(location,month),'allocation'].sum()
                    if august_forecast_local>0:
                        demand_response = np.round(august_forecast_lse / august_forecast_local * august_demand_response_allocation,decimals=2)
                    else:
                        demand_response = 0
                else:
                    demand_response = 0
                return demand_response

            return pd.Series({
                'organization_id' : organization_id,
                'np26_ra_obligation' : np26_ra_obligation,
                'sn_path26_allocation' : sn_path26_allocation,
                'sp26_ra_obligation' : sp26_ra_obligation,
                'ns_path26_allocation' : ns_path26_allocation,
                'year_ahead_flex_rar_category1' : flexibility_requirements.loc[(organization_id,1,filing_month),'flexibility_requirement'] - flexibility_rmr.loc[(organization_id,filing_month),'flexibility_rmr'],
                'year_ahead_flex_rar_category2' : flexibility_requirements.loc[(organization_id,2,filing_month),'flexibility_requirement'],
                'year_ahead_flex_rar_category3' : flexibility_requirements.loc[(organization_id,3,filing_month),'flexibility_requirement'],
                'year_ahead_flex_incremental_category1' : incremental_flex_by_category(1),
                'year_ahead_flex_incremental_category2' : incremental_flex_by_category(2),
                'year_ahead_flex_incremental_category3' : incremental_flex_by_category(3),
                'los_angeles_local_rar' : local_rar.loc[organization_id,'los_angeles'],
                'ventura_local_rar' : local_rar.loc[organization_id,'ventura'],
                'san_diego_local_rar' : local_rar.loc[organization_id,'san_diego'],
                'bay_area_local_rar' : local_rar.loc[organization_id,'bay_area'],
                'fresno_local_rar' : local_rar.loc[organization_id,'fresno'],
                'sierra_local_rar' : local_rar.loc[organization_id,'sierra'],
                'stockton_local_rar' : local_rar.loc[organization_id,'stockton'],
                'kern_local_rar' : local_rar.loc[organization_id,'kern'],
                'humboldt_local_rar' : local_rar.loc[organization_id,'humboldt'],
                'northern_california_local_rar' : local_rar.loc[organization_id,'northern_california'],
                'los_angeles_august_demand_response' : august_demand_response('SCE','la_basin1'),
                'ventura_august_demand_response' : august_demand_response('SCE','big_creek/ventura1'),
                'san_diego_august_demand_response' : august_demand_response('SDGE','sdge1'),
                'bay_area_august_demand_response' : august_demand_response('PGE','bay_area1'),
                'fresno_august_demand_response' : august_demand_response('PGE','fresno'),
                'sierra_august_demand_response' : august_demand_response('PGE','sierra'),
                'stockton_august_demand_response' : august_demand_response('PGE','stockton'),
                'kern_august_demand_response' : august_demand_response('PGE','kern'),
                'humboldt_august_demand_response' : august_demand_response('PGE','humboldt'),
                'northern_california_august_demand_response' : august_demand_response('PGE','ncnb'),
                'los_angeles_incremental_load' : incremental_load_by_area('los_angeles'),
                'ventura_incremental_load' : incremental_load_by_area('ventura'),
                'san_diego_incremental_load' : incremental_load_by_area('san_diego'),
                'bay_area_incremental_load' : incremental_load_by_area('bay_area'),
                'fresno_incremental_load' : incremental_load_by_area('fresno'),
                'sierra_incremental_load' : incremental_load_by_area('sierra'),
                'stockton_incremental_load' : incremental_load_by_area('stockton'),
                'kern_incremental_load' : incremental_load_by_area('kern'),
                'humboldt_incremental_load' : incremental_load_by_area('humboldt'),
                'northern_california_incremental_load' : incremental_load_by_area('northern_california'),
            })
        summary = summary.merge(summary.apply(calculate_summary,axis='columns'),on='organization_id')

        #  copy summary dataframe to worksheet:
        row_number = 2
        procurement = '=SUMIFS(PhysicalResources!E:E,PhysicalResources!A:A,@INDIRECT("A"&ROW()),' + \
            'PhysicalResources!K:K,"{}")+' + \
            'IF(OR(@INDIRECT("A"&ROW())="PGE",' + \
            '@INDIRECT("A"&ROW())="SCE",' + \
            '@INDIRECT("A"&ROW())="SDGE"),0,' + \
            'SUMIFS(PhysicalResources!E:E,PhysicalResources!A:A,@INDIRECT("A"&ROW()),' + \
            'PhysicalResources!K:K,3,PhysicalResources!F:F,"DR")*0.076)'
        compliance_check = '=IF(@INDIRECT(ADDRESS(ROW(),COLUMN()-1))+' + \
           '@INDIRECT(ADDRESS(ROW(),COLUMN()-2))-' + \
            '@INDIRECT(ADDRESS(ROW(),COLUMN()-4))-' + \
            '@INDIRECT(ADDRESS(ROW(),COLUMN()-3))>=0,' + \
            '"compliant",' + \
            '@INDIRECT(ADDRESS(ROW(),COLUMN()-1))+' + \
            '@INDIRECT(ADDRESS(ROW(),COLUMN()-2))-' + \
            '@INDIRECT(ADDRESS(ROW(),COLUMN()-4))-' + \
            '@INDIRECT(ADDRESS(ROW(),COLUMN()-3)))'
        for _,s in summary.iterrows():
            organization_id = s.loc['organization_id']
            ra_summary['NP26']['A{}'.format(row_number)].value = organization_id
            ra_summary['NP26']['B{}'.format(row_number)].value = s.loc['np26_ra_obligation']
            ra_summary['NP26']['B{}'.format(row_number)].number_format = '0.00'
            ra_summary['NP26']['E{}'.format(row_number)].value = s.loc['sn_path26_allocation']
            ra_summary['SP26']['A{}'.format(row_number)].value = organization_id
            ra_summary['SP26']['B{}'.format(row_number)].value = s.loc['sp26_ra_obligation']
            ra_summary['SP26']['B{}'.format(row_number)].number_format = '0.00'
            ra_summary['SP26']['E{}'.format(row_number)].value = s.loc['ns_path26_allocation']
            ra_summary['FlexRAR']['A{}'.format(row_number)].value = organization_id
            ra_summary['FlexRAR']['B{}'.format(row_number)] = '=@INDIRECT("E"&ROW())+@INDIRECT("G"&ROW())+@INDIRECT("I"&ROW())'
            ra_summary['FlexRAR']['C{}'.format(row_number)] = '=@INDIRECT("F"&ROW())+@INDIRECT("H"&ROW())+@INDIRECT("J"&ROW())'
            ra_summary['FlexRAR']['D{}'.format(row_number)] = '=IFERROR(@INDIRECT("C"&ROW())/@INDIRECT("B"&ROW()),0)'
            ra_summary['FlexRAR']['D{}'.format(row_number)].number_format = '0.00%'
            ra_summary['FlexRAR']['E{}'.format(row_number)] = '=@INDIRECT("L"&ROW())+@INDIRECT("Q"&ROW())'
            ra_summary['FlexRAR']['G{}'.format(row_number)] = '=@INDIRECT("M"&ROW())+@INDIRECT("R"&ROW())'
            ra_summary['FlexRAR']['H{}'.format(row_number)] = '=MIN(@INDIRECT("G"&ROW())+@INDIRECT("I"&ROW()),SUMIFS(PhysicalResources!$H:$H,PhysicalResources!$A:$A,@INDIRECT("A"&ROW()), PhysicalResources!$I:$I,2))'
            ra_summary['FlexRAR']['I{}'.format(row_number)] = '=@INDIRECT("N"&ROW())+@INDIRECT("S"&ROW())'
            ra_summary['FlexRAR']['J{}'.format(row_number)] = '=MIN(@INDIRECT("I"&ROW()),SUMIFS(PhysicalResources!$H:$H,PhysicalResources!$A:$A,@INDIRECT("A"&ROW()), PhysicalResources!$I:$I,3))'
            ra_summary['FlexRAR']['L{}'.format(row_number)].value = s.loc['year_ahead_flex_rar_category1']
            ra_summary['FlexRAR']['M{}'.format(row_number)].value = s.loc['year_ahead_flex_rar_category2']
            ra_summary['FlexRAR']['N{}'.format(row_number)].value = s.loc['year_ahead_flex_rar_category3']
            ra_summary['FlexRAR']['O{}'.format(row_number)] = '=@INDIRECT("L"&ROW())+@INDIRECT("M"&ROW())+@INDIRECT("N"&ROW())'
            ra_summary['FlexRAR']['Q{}'.format(row_number)].value = s.loc['year_ahead_flex_incremental_category1']
            ra_summary['FlexRAR']['R{}'.format(row_number)].value = s.loc['year_ahead_flex_incremental_category2']
            ra_summary['FlexRAR']['S{}'.format(row_number)].value = s.loc['year_ahead_flex_incremental_category3']
            ra_summary['FlexRAR']['T{}'.format(row_number)] = '=@INDIRECT("Q"&ROW())+@INDIRECT("R"&ROW())+@INDIRECT("S"&ROW())'
            ra_summary['MCC_Check']['A{}'.format(row_number)] = organization_id
            ra_summary['CertifyingOfficers']['A{}'.format(row_number)] = organization_id
            ra_summary['LocalTrueUp']['A{}'.format(row_number)].value = organization_id
            # los angeles basin local area:
            ra_summary['LocalTrueUp']['B{}'.format(row_number)].value = s.loc['los_angeles_local_rar']
            ra_summary['LocalTrueUp']['C{}'.format(row_number)].value = s.loc['los_angeles_incremental_load']
            ra_summary['LocalTrueUp']['D{}'.format(row_number)].value = s.loc['los_angeles_august_demand_response']
            ra_summary['LocalTrueUp']['E{}'.format(row_number)] = procurement.format('LA Basin')
            ra_summary['LocalTrueUp']['F{}'.format(row_number)] = compliance_check
            # big creek and ventura local area:
            ra_summary['LocalTrueUp']['G{}'.format(row_number)].value = s.loc['ventura_local_rar']
            ra_summary['LocalTrueUp']['H{}'.format(row_number)].value = s.loc['ventura_incremental_load']
            ra_summary['LocalTrueUp']['I{}'.format(row_number)].value = s.loc['ventura_august_demand_response']
            ra_summary['LocalTrueUp']['J{}'.format(row_number)] = procurement.format('Big Creek-Ventura')
            ra_summary['LocalTrueUp']['K{}'.format(row_number)] = compliance_check
            # san diego local area:
            ra_summary['LocalTrueUp']['L{}'.format(row_number)].value = s.loc['san_diego_local_rar']
            ra_summary['LocalTrueUp']['M{}'.format(row_number)].value = s.loc['san_diego_incremental_load']
            ra_summary['LocalTrueUp']['N{}'.format(row_number)].value = s.loc['san_diego_august_demand_response']
            ra_summary['LocalTrueUp']['O{}'.format(row_number)] = procurement.format('San Diego-IV')
            ra_summary['LocalTrueUp']['P{}'.format(row_number)] = compliance_check
            # san francisco bay local area:
            ra_summary['LocalTrueUp']['Q{}'.format(row_number)].value = s.loc['bay_area_local_rar']
            ra_summary['LocalTrueUp']['R{}'.format(row_number)].value = s.loc['bay_area_incremental_load']
            ra_summary['LocalTrueUp']['S{}'.format(row_number)].value = s.loc['bay_area_august_demand_response']
            ra_summary['LocalTrueUp']['T{}'.format(row_number)] = procurement.format('Bay Area')
            ra_summary['LocalTrueUp']['U{}'.format(row_number)] = compliance_check
            # humboldt county local area:
            ra_summary['LocalTrueUp']['V{}'.format(row_number)].value = s.loc['humboldt_local_rar']
            ra_summary['LocalTrueUp']['W{}'.format(row_number)].value = s.loc['humboldt_incremental_load']
            ra_summary['LocalTrueUp']['X{}'.format(row_number)].value = s.loc['humboldt_august_demand_response']
            ra_summary['LocalTrueUp']['Y{}'.format(row_number)] = procurement.format('Humboldt')
            ra_summary['LocalTrueUp']['Z{}'.format(row_number)] = compliance_check
            # sierra local area:
            ra_summary['LocalTrueUp']['AA{}'.format(row_number)].value = s.loc['sierra_local_rar']
            ra_summary['LocalTrueUp']['AB{}'.format(row_number)].value = s.loc['sierra_incremental_load']
            ra_summary['LocalTrueUp']['AC{}'.format(row_number)].value = s.loc['sierra_august_demand_response']
            ra_summary['LocalTrueUp']['AD{}'.format(row_number)] = procurement.format('Sierra')
            ra_summary['LocalTrueUp']['AE{}'.format(row_number)] = compliance_check
            # stockton local area:
            ra_summary['LocalTrueUp']['AF{}'.format(row_number)].value = s.loc['stockton_local_rar']
            ra_summary['LocalTrueUp']['AG{}'.format(row_number)].value = s.loc['stockton_incremental_load']
            ra_summary['LocalTrueUp']['AH{}'.format(row_number)].value = s.loc['stockton_august_demand_response']
            ra_summary['LocalTrueUp']['AI{}'.format(row_number)] = procurement.format('Stockton')
            ra_summary['LocalTrueUp']['AJ{}'.format(row_number)] = compliance_check
            # northern california and north bay local area:
            ra_summary['LocalTrueUp']['AK{}'.format(row_number)].value = s.loc['northern_california_local_rar']
            ra_summary['LocalTrueUp']['AL{}'.format(row_number)].value = s.loc['northern_california_incremental_load']
            ra_summary['LocalTrueUp']['AM{}'.format(row_number)].value = s.loc['northern_california_august_demand_response']
            ra_summary['LocalTrueUp']['AN{}'.format(row_number)] = procurement.format('NCNB')
            ra_summary['LocalTrueUp']['AO{}'.format(row_number)] = compliance_check
            # fresno local area:
            ra_summary['LocalTrueUp']['AP{}'.format(row_number)].value = s.loc['fresno_local_rar']
            ra_summary['LocalTrueUp']['AQ{}'.format(row_number)].value = s.loc['fresno_incremental_load']
            ra_summary['LocalTrueUp']['AR{}'.format(row_number)].value = s.loc['fresno_august_demand_response']
            ra_summary['LocalTrueUp']['AS{}'.format(row_number)] = procurement.format('Fresno')
            ra_summary['LocalTrueUp']['AT{}'.format(row_number)] = compliance_check
            # kern county local area:
            ra_summary['LocalTrueUp']['AU{}'.format(row_number)].value = s.loc['kern_local_rar']
            ra_summary['LocalTrueUp']['AV{}'.format(row_number)].value = s.loc['kern_incremental_load']
            ra_summary['LocalTrueUp']['AW{}'.format(row_number)].value = s.loc['kern_august_demand_response']
            ra_summary['LocalTrueUp']['AX{}'.format(row_number)] = procurement.format('Kern')
            ra_summary['LocalTrueUp']['AY{}'.format(row_number)] = compliance_check
            # pge other aggregated:
            ra_summary['LocalTrueUp']['AZ{}'.format(row_number)] = '=@INDIRECT("V"&ROW())+@INDIRECT("AA"&ROW())+@INDIRECT("AF"&ROW())+@INDIRECT("AK"&ROW())+@INDIRECT("AP"&ROW())+@INDIRECT("AU"&ROW())'
            ra_summary['LocalTrueUp']['BA{}'.format(row_number)] = '=@INDIRECT("W"&ROW())+@INDIRECT("AB"&ROW())+@INDIRECT("AG"&ROW())+@INDIRECT("AL"&ROW())+@INDIRECT("AQ"&ROW())+@INDIRECT("AV"&ROW())'
            ra_summary['LocalTrueUp']['BB{}'.format(row_number)] = '=@INDIRECT("X"&ROW())+@INDIRECT("AC"&ROW())+@INDIRECT("AH"&ROW())+@INDIRECT("AM"&ROW())+@INDIRECT("AR"&ROW())+@INDIRECT("AW"&ROW())'
            ra_summary['LocalTrueUp']['BC{}'.format(row_number)] = '=@INDIRECT("Y"&ROW())+@INDIRECT("AD"&ROW())+@INDIRECT("AI"&ROW())+@INDIRECT("AN"&ROW())+@INDIRECT("AS"&ROW())+@INDIRECT("AX"&ROW())'
            ra_summary['LocalTrueUp']['BD{}'.format(row_number)] = '=@INDIRECT("Z"&ROW())+@INDIRECT("AE"&ROW())+@INDIRECT("AJ"&ROW())+@INDIRECT("AO"&ROW())+@INDIRECT("AT"&ROW())+@INDIRECT("AY"&ROW())'
            # sce service territory:
            ra_summary['LocalTrueUp']['BE{}'.format(row_number)] = '=@INDIRECT("B"&ROW())+@INDIRECT("G"&ROW())'
            ra_summary['LocalTrueUp']['BF{}'.format(row_number)] = '=@INDIRECT("C"&ROW())+@INDIRECT("H"&ROW())'
            ra_summary['LocalTrueUp']['BG{}'.format(row_number)] = '=@INDIRECT("D"&ROW())+@INDIRECT("I"&ROW())'
            ra_summary['LocalTrueUp']['BH{}'.format(row_number)] = '=@INDIRECT("E"&ROW())+@INDIRECT("J"&ROW())'
            ra_summary['LocalTrueUp']['BI{}'.format(row_number)] = '=@INDIRECT("F"&ROW())+@INDIRECT("K"&ROW())'
            # sdge service territory:
            ra_summary['LocalTrueUp']['BJ{}'.format(row_number)] = '=@INDIRECT("L"&ROW())'
            ra_summary['LocalTrueUp']['BK{}'.format(row_number)] = '=@INDIRECT("M"&ROW())'
            ra_summary['LocalTrueUp']['BL{}'.format(row_number)] = '=@INDIRECT("N"&ROW())'
            ra_summary['LocalTrueUp']['BM{}'.format(row_number)] = '=@INDIRECT("O"&ROW())'
            ra_summary['LocalTrueUp']['BN{}'.format(row_number)] = '=@INDIRECT("P"&ROW())'
            # pge service territory:
            ra_summary['LocalTrueUp']['BO{}'.format(row_number)] = '=@INDIRECT("Q"&ROW())+@INDIRECT("V"&ROW())+@INDIRECT("AA"&ROW())+@INDIRECT("AF"&ROW())+@INDIRECT("AK"&ROW())+@INDIRECT("AP"&ROW())+@INDIRECT("AU"&ROW())'
            ra_summary['LocalTrueUp']['BP{}'.format(row_number)] = '=@INDIRECT("R"&ROW())+@INDIRECT("W"&ROW())+@INDIRECT("AB"&ROW())+@INDIRECT("AG"&ROW())+@INDIRECT("AL"&ROW())+@INDIRECT("AQ"&ROW())+@INDIRECT("AV"&ROW())'
            ra_summary['LocalTrueUp']['BQ{}'.format(row_number)] = '=@INDIRECT("S"&ROW())+@INDIRECT("X"&ROW())+@INDIRECT("AC"&ROW())+@INDIRECT("AH"&ROW())+@INDIRECT("AM"&ROW())+@INDIRECT("AR"&ROW())+@INDIRECT("AW"&ROW())'
            ra_summary['LocalTrueUp']['BR{}'.format(row_number)] = '=@INDIRECT("T"&ROW())+@INDIRECT("Y"&ROW())+@INDIRECT("AD"&ROW())+@INDIRECT("AI"&ROW())+@INDIRECT("AN"&ROW())+@INDIRECT("AS"&ROW())+@INDIRECT("AX"&ROW())'
            ra_summary['LocalTrueUp']['BS{}'.format(row_number)] = '=@INDIRECT("U"&ROW())+@INDIRECT("Z"&ROW())+@INDIRECT("AE"&ROW())+@INDIRECT("AJ"&ROW())+@INDIRECT("AO"&ROW())+@INDIRECT("AT"&ROW())+@INDIRECT("AY"&ROW())'

            # caiso supply plan cross-check:
            caiso_cross_check['Requirements']['A{}'.format(row_number+1)] = organization_id
            caiso_cross_check['Requirements']['B{}'.format(row_number+1)].value = s.loc[['np26_ra_obligation','sp26_ra_obligation']].sum()
            caiso_cross_check['Requirements']['B{}'.format(row_number+1)].number_format = '0.00'
            caiso_cross_check['Requirements']['H{}'.format(row_number+1)] = '=@INDIRECT("K"&ROW())+@INDIRECT("M"&ROW())+@INDIRECT("O"&ROW())'
            caiso_cross_check['Requirements']['I{}'.format(row_number+1)] = '=@INDIRECT("L"&ROW())+@INDIRECT("N"&ROW())+@INDIRECT("P"&ROW())'
            caiso_cross_check['Requirements']['J{}'.format(row_number+1)] = '=IFERROR(@INDIRECT("I"&ROW())/@INDIRECT("H"&ROW()),0)'
            caiso_cross_check['Requirements']['J{}'.format(row_number+1)].number_format = '0.00%'
            caiso_cross_check['Requirements']['K{}'.format(row_number+1)].value = s.loc[['year_ahead_flex_rar_category1','year_ahead_flex_incremental_category1']].sum()
            caiso_cross_check['Requirements']['M{}'.format(row_number+1)].value = s.loc[['year_ahead_flex_rar_category2','year_ahead_flex_incremental_category2']].sum()
            caiso_cross_check['Requirements']['O{}'.format(row_number+1)].value = s.loc[['year_ahead_flex_rar_category3','year_ahead_flex_incremental_category3']].sum()
            caiso_cross_check['Requirements']['Q{}'.format(row_number+1)].value = s.loc['year_ahead_flex_rar_category1']
            caiso_cross_check['Requirements']['R{}'.format(row_number+1)].value = s.loc['year_ahead_flex_rar_category2']
            caiso_cross_check['Requirements']['S{}'.format(row_number+1)].value = s.loc['year_ahead_flex_rar_category3']
            caiso_cross_check['Requirements']['T{}'.format(row_number+1)].value = s.loc[['year_ahead_flex_rar_category1','year_ahead_flex_rar_category2','year_ahead_flex_rar_category3']].sum()
            # los angeles basin local area:
            caiso_cross_check['Requirements']['V{}'.format(row_number+1)].value = s.loc['los_angeles_local_rar']
            caiso_cross_check['Requirements']['W{}'.format(row_number+1)].value = s.loc['los_angeles_incremental_load']
            caiso_cross_check['Requirements']['X{}'.format(row_number+1)].value = s.loc['los_angeles_august_demand_response']
            caiso_cross_check['Requirements']['Z{}'.format(row_number+1)] = compliance_check
            # big creek and ventura local area:
            caiso_cross_check['Requirements']['AA{}'.format(row_number+1)].value = s.loc['ventura_local_rar']
            caiso_cross_check['Requirements']['AB{}'.format(row_number+1)].value = s.loc['ventura_incremental_load']
            caiso_cross_check['Requirements']['AC{}'.format(row_number+1)].value = s.loc['ventura_august_demand_response']
            caiso_cross_check['Requirements']['AE{}'.format(row_number+1)] = compliance_check
            # san diego local area:
            caiso_cross_check['Requirements']['AF{}'.format(row_number+1)].value = s.loc['san_diego_local_rar']
            caiso_cross_check['Requirements']['AG{}'.format(row_number+1)].value = s.loc['san_diego_incremental_load']
            caiso_cross_check['Requirements']['AH{}'.format(row_number+1)].value = s.loc['san_diego_august_demand_response']
            caiso_cross_check['Requirements']['AJ{}'.format(row_number+1)] = compliance_check
            # san francisco bay local area:
            caiso_cross_check['Requirements']['AK{}'.format(row_number+1)].value = s.loc['bay_area_local_rar']
            caiso_cross_check['Requirements']['AL{}'.format(row_number+1)].value = s.loc['bay_area_incremental_load']
            caiso_cross_check['Requirements']['AM{}'.format(row_number+1)].value = s.loc['bay_area_august_demand_response']
            caiso_cross_check['Requirements']['AO{}'.format(row_number+1)] = compliance_check
            # humboldt county local area:
            caiso_cross_check['Requirements']['AP{}'.format(row_number+1)].value = s.loc['humboldt_local_rar']
            caiso_cross_check['Requirements']['AQ{}'.format(row_number+1)].value = s.loc['humboldt_incremental_load']
            caiso_cross_check['Requirements']['AR{}'.format(row_number+1)].value = s.loc['humboldt_august_demand_response']
            caiso_cross_check['Requirements']['AT{}'.format(row_number+1)] = compliance_check
            # sierra local area:
            caiso_cross_check['Requirements']['AU{}'.format(row_number+1)].value = s.loc['sierra_local_rar']
            caiso_cross_check['Requirements']['AV{}'.format(row_number+1)].value = s.loc['sierra_incremental_load']
            caiso_cross_check['Requirements']['AW{}'.format(row_number+1)].value = s.loc['sierra_august_demand_response']
            caiso_cross_check['Requirements']['AY{}'.format(row_number+1)] = compliance_check
            # stockton local area:
            caiso_cross_check['Requirements']['AZ{}'.format(row_number+1)].value = s.loc['stockton_local_rar']
            caiso_cross_check['Requirements']['BA{}'.format(row_number+1)].value = s.loc['stockton_incremental_load']
            caiso_cross_check['Requirements']['BB{}'.format(row_number+1)].value = s.loc['stockton_august_demand_response']
            caiso_cross_check['Requirements']['BD{}'.format(row_number+1)] = compliance_check
            # northern california and north bay local area:
            caiso_cross_check['Requirements']['BE{}'.format(row_number+1)].value = s.loc['northern_california_local_rar']
            caiso_cross_check['Requirements']['BF{}'.format(row_number+1)].value = s.loc['northern_california_incremental_load']
            caiso_cross_check['Requirements']['BG{}'.format(row_number+1)].value = s.loc['northern_california_august_demand_response']
            caiso_cross_check['Requirements']['BI{}'.format(row_number+1)] = compliance_check
            # fresno local area:
            caiso_cross_check['Requirements']['BJ{}'.format(row_number+1)].value = s.loc['fresno_local_rar']
            caiso_cross_check['Requirements']['BK{}'.format(row_number+1)].value = s.loc['fresno_incremental_load']
            caiso_cross_check['Requirements']['BL{}'.format(row_number+1)].value = s.loc['fresno_august_demand_response']
            caiso_cross_check['Requirements']['BN{}'.format(row_number+1)] = compliance_check
            # kern county local area:
            caiso_cross_check['Requirements']['BO{}'.format(row_number+1)].value = s.loc['kern_local_rar']
            caiso_cross_check['Requirements']['BP{}'.format(row_number+1)].value = s.loc['kern_incremental_load']
            caiso_cross_check['Requirements']['BQ{}'.format(row_number+1)].value = s.loc['kern_august_demand_response']
            caiso_cross_check['Requirements']['BS{}'.format(row_number+1)] = compliance_check
            # pge other aggregated:
            caiso_cross_check['Requirements']['BT{}'.format(row_number+1)].value = s.loc[['humboldt_local_rar','sierra_local_rar','stockton_local_rar','northern_california_local_rar','fresno_local_rar','kern_local_rar']].sum()
            caiso_cross_check['Requirements']['BU{}'.format(row_number+1)].value = s.loc[['humboldt_incremental_load','sierra_incremental_load','stockton_incremental_load','northern_california_incremental_load','fresno_incremental_load','kern_incremental_load']].sum()
            caiso_cross_check['Requirements']['BV{}'.format(row_number+1)].value = s.loc[['humboldt_august_demand_response','sierra_august_demand_response','stockton_august_demand_response','northern_california_august_demand_response','fresno_august_demand_response','kern_august_demand_response']].sum()
            caiso_cross_check['Requirements']['BX{}'.format(row_number+1)] = compliance_check
            # sce service territory:
            caiso_cross_check['Requirements']['BY{}'.format(row_number+1)] = s.loc[['los_angeles_local_rar','ventura_local_rar']].sum()
            caiso_cross_check['Requirements']['BZ{}'.format(row_number+1)] = s.loc[['los_angeles_incremental_load','ventura_incremental_load']].sum()
            caiso_cross_check['Requirements']['CA{}'.format(row_number+1)] = s.loc[['los_angeles_august_demand_response','ventura_august_demand_response']].sum()
            caiso_cross_check['Requirements']['CC{}'.format(row_number+1)] = compliance_check
            # sdge service territory:
            caiso_cross_check['Requirements']['CD{}'.format(row_number+1)] = s.loc['san_diego_local_rar']
            caiso_cross_check['Requirements']['CE{}'.format(row_number+1)] = s.loc['san_diego_incremental_load']
            caiso_cross_check['Requirements']['CF{}'.format(row_number+1)] = s.loc['san_diego_august_demand_response']
            caiso_cross_check['Requirements']['CH{}'.format(row_number+1)] = compliance_check
            # pge service territory:
            caiso_cross_check['Requirements']['CI{}'.format(row_number+1)] = s.loc[['bay_area_local_rar','humboldt_local_rar','sierra_local_rar','stockton_local_rar','northern_california_local_rar','fresno_local_rar','kern_local_rar']].sum()
            caiso_cross_check['Requirements']['CJ{}'.format(row_number+1)] = s.loc[['bay_area_incremental_load','humboldt_incremental_load','sierra_incremental_load','stockton_incremental_load','northern_california_incremental_load','fresno_incremental_load','kern_incremental_load']].sum()
            caiso_cross_check['Requirements']['CK{}'.format(row_number+1)] = s.loc[['bay_area_august_demand_response','humboldt_august_demand_response','sierra_august_demand_response','stockton_august_demand_response','northern_california_august_demand_response','fresno_august_demand_response','kern_august_demand_response']].sum()
            caiso_cross_check['Requirements']['CM{}'.format(row_number+1)] = compliance_check

            row_number += 1
        self.consolidation_logger.commit()

        # total rows on local true-up sheet:
        ra_summary['LocalTrueUp']['A{}'.format(row_number)] = 'Total:'
        for col in map(get_column_letter,range(2,72)):
            ra_summary['LocalTrueUp']['{}{}'.format(col,row_number)] = '=SUM(INDIRECT(ADDRESS(2,COLUMN())&":"&ADDRESS(ROW()-1,COLUMN())))'

        # save and close summary and caiso supply plan cross-check files:
        ra_summary.save(str(self.paths.get_path('ra_summary')))
        ra_summary.close()
        caiso_cross_check.save(str(self.paths.get_path('caiso_cross_check')))
        caiso_cross_check.close()

        # check time and report:
        run_time = (ts.now() - init_time).total_seconds()
        self.logger.log('Retrieved Allocations in {:02.0f}:{:02.0f}:{:05.2f}'.format(int(run_time/3600),int((run_time%3600)/60),run_time%60),'INFORMATION')

    # collect data from each of the monthly lse filings:
    def consolidate_filings(self):
        # start timer:
        init_time = ts.now()
        self.logger.log('Consolidating Data from LSE Filings','INFORMATION')

        # get list of active load serving entities from summary sheet:
        path = self.paths.get_path('ra_summary')
        ra_summary = open_workbook(path,data_only=False,read_only=False)
        data_range = get_data_range(ra_summary['Summary'],'A','',self.organizations)
        active_organizations = [row[0].value for row in data_range]

        # open summary from previous month:
        path = self.paths.get_path('ra_summary_previous_month')
        ra_summary_previous_month = open_workbook(path,data_only=False,read_only=True)

        # open caiso supply plan cross-check file:
        path = self.paths.get_path('caiso_cross_check')
        caiso_cross_check = open_workbook(path,data_only=False,read_only=False)

        # initialize tables:
        summary = pd.DataFrame()
        physical_resources = pd.DataFrame()
        demand_response = pd.DataFrame()

        # combine data tables from each lse filing:
        for organization_id in active_organizations:
            organization = self.organizations.get_organization(organization_id)
            ra_monthly_filing_data = read_ra_monthly_filing(organization,self.paths,self.logger)

            # append summary, physical resources, and demand response tables with lse-specific data:
            summary = pd.concat([summary,ra_monthly_filing_data['summary']],axis='index',ignore_index=True)
            physical_resources = pd.concat([physical_resources,ra_monthly_filing_data['physical_resources']],axis='index',ignore_index=True)
            demand_response = pd.concat([demand_response,ra_monthly_filing_data['demand_response']],axis='index',ignore_index=True)

        # set summary table index:
        summary.set_index('organization_id',inplace=True)
        summary.sort_index(inplace=True)

        # get the nqc list from the summary file:
        nqc_list = get_nqc_list(ra_summary,self.configuration_options.get_option('filing_month'))

        # helper function to retrieve zone for a given resource id:
        def get_zone(resource_id: str):
            if resource_id in nqc_list.index:
                zone = nqc_list.loc[resource_id,'zone']
            else:
                zone = 'Unknown'
            return zone

        # set the nqc list index:
        nqc_list.set_index('resource_id',inplace=True)
        nqc_list.sort_index(inplace=True)

        # get certifications from previous month:
        columns = ['organization_id','organization_officer_name','organization_officer_title']
        try:
            data_range = get_data_range(ra_summary_previous_month['CertifyingOfficers'],'A','BC',self.organizations)
            previous_certifications = data_range_to_dataframe(columns,data_range)
        except:
            previous_certifications = pd.DataFrame(columns=columns)
        previous_certifications.set_index('organization_id',inplace=True)
        previous_certifications.sort_index(inplace=True)
            
        # write consolidated data and formulas to summary and caiso check files:
        first_row_number_summary = 2
        row_number_summary = first_row_number_summary
        first_row_number_physical_resources = 2
        row_number_physical_resources = first_row_number_physical_resources
        for organization_id in active_organizations:
            ra_summary['NP26']['C{}'.format(row_number_summary)] = '=SUMIFS(PhysicalResources!D:D,PhysicalResources!A:A,@INDIRECT("A"&ROW()),PhysicalResources!J:J,"North",PhysicalResources!F:F,"<>DR")'
            ra_summary['NP26']['C{}'.format(row_number_summary)].fill = PatternFill(start_color='FFCC99',end_color='FFCC99',fill_type='solid')
            ra_summary['NP26']['C{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['NP26']['D{}'.format(row_number_summary)] = '=ROUND(@INDIRECT("H"&ROW()),2)'
            ra_summary['NP26']['D{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['NP26']['E{}'.format(row_number_summary)].value = 0
            ra_summary['NP26']['E{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['NP26']['F{}'.format(row_number_summary)] = '=MAX(@INDIRECT("B"&ROW())-@INDIRECT("C"&ROW())-@INDIRECT("D"&ROW())-@INDIRECT("E"&ROW()),0)'
            ra_summary['NP26']['F{}'.format(row_number_summary)].fill = PatternFill(start_color='CC99FF',end_color='CC99FF',fill_type='solid')
            ra_summary['NP26']['F{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['NP26']['G{}'.format(row_number_summary)].value = summary.loc[organization_id,'np26dr']
            ra_summary['NP26']['G{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['NP26']['H{}'.format(row_number_summary)] = '={}*@INDIRECT("G"&ROW())'.format(1+self.configuration_options.get_option('planning_reserve_margin'))
            ra_summary['NP26']['H{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['NP26']['I{}'.format(row_number_summary)] = '=@INDIRECT("C"&ROW())+@INDIRECT("D"&ROW())'
            ra_summary['NP26']['I{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['NP26']['K{}'.format(row_number_summary)] = '=@INDIRECT("C"&ROW())+@INDIRECT("D"&ROW())-@INDIRECT("B"&ROW())'
            ra_summary['NP26']['K{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['NP26']['L{}'.format(row_number_summary)] = '=@INDIRECT("K"&ROW())+@INDIRECT("SP26!K"&ROW())'
            ra_summary['NP26']['L{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['NP26']['M{}'.format(row_number_summary)] = '=@INDIRECT("L"&ROW())-@INDIRECT("SP26!L"&ROW())'
            ra_summary['NP26']['M{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['SP26']['C{}'.format(row_number_summary)] = '=SUMIFS(PhysicalResources!D:D,PhysicalResources!A:A,@INDIRECT("A"&ROW()),PhysicalResources!J:J,"South",PhysicalResources!F:F, "<>DR")'
            ra_summary['SP26']['C{}'.format(row_number_summary)].fill = PatternFill(start_color='FFCC99',end_color='FFCC99',fill_type='solid')
            ra_summary['SP26']['C{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['SP26']['D{}'.format(row_number_summary)] = '=ROUND(@INDIRECT("H"&ROW()),2)'
            ra_summary['SP26']['D{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['SP26']['E{}'.format(row_number_summary)].value = 0
            ra_summary['SP26']['E{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['SP26']['F{}'.format(row_number_summary)] = '=MAX(@INDIRECT("B"&ROW())-@INDIRECT("C"&ROW())-@INDIRECT("D"&ROW())-@INDIRECT("E"&ROW()),0)'
            ra_summary['SP26']['F{}'.format(row_number_summary)].fill = PatternFill(start_color='CC99FF',end_color='CC99FF',fill_type='solid')
            ra_summary['SP26']['F{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['SP26']['G{}'.format(row_number_summary)] = summary.loc[organization_id,'sp26dr']
            ra_summary['SP26']['G{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['SP26']['H{}'.format(row_number_summary)] = '={}*@INDIRECT("G"&ROW())'.format(1+self.configuration_options.get_option('planning_reserve_margin'))
            ra_summary['SP26']['H{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['SP26']['I{}'.format(row_number_summary)] = '=@INDIRECT("C"&ROW())+@INDIRECT("D"&ROW())'
            ra_summary['SP26']['I{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['SP26']['K{}'.format(row_number_summary)] = '=@INDIRECT("C"&ROW())+@INDIRECT("D"&ROW())-@INDIRECT("B"&ROW())'
            ra_summary['SP26']['K{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['SP26']['L{}'.format(row_number_summary)] = '=@INDIRECT("K"&ROW())+@INDIRECT("NP26!K"&ROW())'
            ra_summary['SP26']['L{}'.format(row_number_summary)].number_format = '0.00'
            ra_summary['MCC_Check']['B{}'.format(row_number_summary)] = '=VLOOKUP(@INDIRECT("A"&ROW()),Summary!$A:$B,2,FALSE)'
            ra_summary['MCC_Check']['C{}'.format(row_number_summary)] = '=@INDIRECT("F"&ROW())+@INDIRECT("L"&ROW())+@INDIRECT("N"&ROW())'
            ra_summary['MCC_Check']['D{}'.format(row_number_summary)] = '=IFERROR(@INDIRECT("C"&ROW())/@INDIRECT("B"&ROW()),"")'
            ra_summary['MCC_Check']['E{}'.format(row_number_summary)] = '=@INDIRECT("B"&ROW())*MCC_Parameters!$B$2'
            ra_summary['MCC_Check']['F{}'.format(row_number_summary)] = '=MIN(@INDIRECT("E"&ROW()),@INDIRECT("NP26!H"&ROW())+@INDIRECT("SP26!H"&ROW()))'
            ra_summary['MCC_Check']['G{}'.format(row_number_summary)] = '=@INDIRECT("B"&ROW())*MCC_Parameters!$B$3'
            ra_summary['MCC_Check']['H{}'.format(row_number_summary)] = '=MIN(@INDIRECT("G"&ROW()),SUMIFS(PhysicalResources!$D:$D,PhysicalResources!$A:$A,@INDIRECT("A"&ROW()),PhysicalResources!$F:$F,1))'
            ra_summary['MCC_Check']['I{}'.format(row_number_summary)] = '=@INDIRECT("B"&ROW())*MCC_Parameters!$B$4'
            ra_summary['MCC_Check']['J{}'.format(row_number_summary)] = '=MIN(@INDIRECT("I"&ROW()),SUMIFS(PhysicalResources!$D:$D,PhysicalResources!$A:$A,@INDIRECT("A"&ROW()),PhysicalResources!$F:$F,2)+@INDIRECT("H"&ROW()))'
            ra_summary['MCC_Check']['K{}'.format(row_number_summary)] = '=@INDIRECT("B"&ROW())*MCC_Parameters!$B$5'
            ra_summary['MCC_Check']['L{}'.format(row_number_summary)] = '=MIN(@INDIRECT("K"&ROW()),SUMIFS(PhysicalResources!$D:$D,PhysicalResources!$A:$A,@INDIRECT("A"&ROW()),PhysicalResources!$F:$F,3)+@INDIRECT("J"&ROW()))'
            ra_summary['MCC_Check']['M{}'.format(row_number_summary)] = '=@INDIRECT("B"&ROW())*MCC_Parameters!$B$6'
            ra_summary['MCC_Check']['N{}'.format(row_number_summary)] = '=SUMIFS(PhysicalResources!$D:$D,PhysicalResources!$A:$A,@INDIRECT("A"&ROW()),PhysicalResources!$F:$F,4)'
            ra_summary['MCC_Check']['O{}'.format(row_number_summary)] = '=@INDIRECT("B"&ROW())*MCC_Parameters!$B$7'
            ra_summary['MCC_Check']['P{}'.format(row_number_summary)] = '=SUMIFS(PhysicalResources!$D:$D,PhysicalResources!$A:$A,@INDIRECT("A"&ROW()),PhysicalResources!$F:$F,4,PhysicalResources!$G:$G,TRUE)'
            ra_summary['FlexRAR']['F{}'.format(row_number_summary)] = '=SUMIFS(PhysicalResources!$H:$H,PhysicalResources!$A:$A,@INDIRECT("A"&ROW()),PhysicalResources!$I:$I,1)'
            ra_summary['CertifyingOfficers']['B{}'.format(row_number_summary)] = summary.loc[organization_id,'organization_officer_name']
            ra_summary['CertifyingOfficers']['C{}'.format(row_number_summary)] = summary.loc[organization_id,'organization_officer_title']
            ra_summary['CertifyingOfficers']['E{}'.format(row_number_summary)] = '=IF(@INDIRECT("D"&ROW())=@INDIRECT("B"&ROW()),"-","Yes")'
            ra_summary['CertifyingOfficers']['G{}'.format(row_number_summary)] = '=IF(@INDIRECT("F"&ROW())=@INDIRECT("C"&ROW()),"-","Yes")'
            if organization_id in previous_certifications.index:
                ra_summary['CertifyingOfficers']['D{}'.format(row_number_summary)] = previous_certifications.loc[organization_id,'organization_officer_name']
                ra_summary['CertifyingOfficers']['F{}'.format(row_number_summary)] = previous_certifications.loc[organization_id,'organization_officer_title']
            else:
                ra_summary['CertifyingOfficers']['D{}'.format(row_number_summary)] = '[Record Not Found]'
                ra_summary['CertifyingOfficers']['F{}'.format(row_number_summary)] = '[Record Not Found]'
            if self.paths.get_path('ra_monthly_filing',self.organizations.get_organization(organization_id)):
                filename = self.paths.get_path('ra_monthly_filing',self.organizations.get_organization(organization_id)).name
            else:
                filename = 'Monthly Filing Not Found'
            ra_summary['Summary']['H{}'.format(row_number_summary)] = filename
            ra_summary['NP26']['M{}'.format(row_number_summary)] = filename
            ra_summary['SP26']['L{}'.format(row_number_summary)] = filename
            ra_summary['CertifyingOfficers']['H{}'.format(row_number_summary)] = filename
            caiso_cross_check['Requirements']['C{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id),'resource_adequacy_system'].sum() - physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'resource_mcc_bucket']=='DR'),'resource_adequacy_system'].sum()
            caiso_cross_check['Requirements']['C{}'.format(row_number_summary+1)].number_format = '0.00'
            caiso_cross_check['Requirements']['D{}'.format(row_number_summary+1)].value = (1 + self.configuration_options.get_option('planning_reserve_margin')) * (summary.loc[organization_id,'np26dr'] + summary.loc[organization_id,'sp26dr'])
            caiso_cross_check['Requirements']['D{}'.format(row_number_summary+1)].number_format = '0.00'
            caiso_cross_check['Requirements']['E{}'.format(row_number_summary+1)] = '=@INDIRECT("C"&ROW())+@INDIRECT("D"&ROW())'
            caiso_cross_check['Requirements']['E{}'.format(row_number_summary+1)].number_format = '0.00'
            caiso_cross_check['Requirements']['F{}'.format(row_number_summary+1)] = '=IFERROR(@INDIRECT("E"&ROW())/@INDIRECT("B"&ROW()),"")'
            caiso_cross_check['Requirements']['F{}'.format(row_number_summary+1)].number_format = '0.00%'
            caiso_cross_check['Requirements']['L{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'resource_adequacy_flexibility_category']==1),'resource_adequacy_committed_flexible'].sum()
            caiso_cross_check['Requirements']['N{}'.format(row_number_summary+1)].value = min(
                caiso_cross_check['Requirements']['M{}'.format(row_number_summary+1)].value +
                caiso_cross_check['Requirements']['O{}'.format(row_number_summary+1)].value,
                physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'resource_adequacy_flexibility_category']==2),'resource_adequacy_committed_flexible'].sum()
            )
            caiso_cross_check['Requirements']['P{}'.format(row_number_summary+1)].value = min(
                caiso_cross_check['Requirements']['O{}'.format(row_number_summary+1)].value,
                physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'resource_adequacy_flexibility_category']==3),'resource_adequacy_committed_flexible'].sum()
            )
            caiso_cross_check['Requirements']['Y{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='LA Basin'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['AD{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='Big Creek-Ventura'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['AI{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='San Diego-IV'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['AN{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='Bay Area'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['AS{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='Humboldt'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['AX{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='Sierra'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['BC{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='Stockton'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['BH{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='NCNB'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['BM{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='Fresno'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['BR{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='Kern'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['BW{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area'].isin(['Humboldt','Sierra','Stockton','NCNB','Fresno','Kern'])),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['CB{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area'].isin(['LA Basin','Big Creek-Ventura'])),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['CG{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area']=='San Diego'),'resource_adequacy_local'].sum()
            caiso_cross_check['Requirements']['CL{}'.format(row_number_summary+1)].value = physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id)&(physical_resources.loc[:,'local_area'].isin(['Bay Area','Humboldt','Sierra','Stockton','NCNB','Fresno','Kern'])),'resource_adequacy_local'].sum()
            caiso_cross_check['FilingSummary']['A{}'.format(row_number_summary)] = organization_id
            caiso_cross_check['FilingSummary']['B{}'.format(row_number_summary)] = '=SUMIF(Filings!$A:$A,@INDIRECT("A"&ROW()),Filings!$G:$G)'
            caiso_cross_check['FilingSummary']['C{}'.format(row_number_summary)] = '=SUMIF(CAISO_Sys_SP!$C:$C,@INDIRECT("A"&ROW()),CAISO_Sys_SP!$I:$I)'
            caiso_cross_check['FilingSummary']['D{}'.format(row_number_summary)] = '=@INDIRECT("C"&ROW())-@INDIRECT("B"&ROW())'
            caiso_cross_check['FilingSummary']['D{}'.format(row_number_summary)].fill = PatternFill(start_color='FFF2CC',end_color='FFF2CC',fill_type='solid')
            caiso_cross_check['FilingSummary']['E{}'.format(row_number_summary)] = '=SUMIFS(Filings!$M:$M,Filings!$A:$A,@INDIRECT("A"&ROW()),Filings!$Q:$Q,1)'
            caiso_cross_check['FilingSummary']['F{}'.format(row_number_summary)] = '=SUMIFS(CAISO_Flex_SP!$G:$G,CAISO_Flex_SP!$B:$B,@INDIRECT("A"&ROW()),CAISO_Flex_SP!$F:$F,1)'
            caiso_cross_check['FilingSummary']['G{}'.format(row_number_summary)] = '=@INDIRECT("F"&ROW())-@INDIRECT("E"&ROW())'
            caiso_cross_check['FilingSummary']['G{}'.format(row_number_summary)].fill = PatternFill(start_color='FFF2CC',end_color='FFF2CC',fill_type='solid')
            caiso_cross_check['FilingSummary']['H{}'.format(row_number_summary)] = '=SUMIFS(Filings!$M:$M,Filings!$A:$A,@INDIRECT("A"&ROW()),Filings!$Q:$Q,2)'
            caiso_cross_check['FilingSummary']['I{}'.format(row_number_summary)] = '=SUMIFS(CAISO_Flex_SP!$G:$G,CAISO_Flex_SP!$B:$B,@INDIRECT("A"&ROWC()),CAISO_Flex_SP!$F:$F,2)'
            caiso_cross_check['FilingSummary']['J{}'.format(row_number_summary)] = '=@INDIRECT("I"&ROW())-@INDIRECT("H"&ROW())'
            caiso_cross_check['FilingSummary']['J{}'.format(row_number_summary)].fill = PatternFill(start_color='FFF2CC',end_color='FFF2CC',fill_type='solid')
            caiso_cross_check['FilingSummary']['K{}'.format(row_number_summary)] = '=SUMIFS(Filings!$M:$M,Filings!$A:$A,@INDIRECT("A"&ROW()),Filings!$Q:$Q,3)'
            caiso_cross_check['FilingSummary']['L{}'.format(row_number_summary)] = '=SUMIFS(CAISO_Flex_SP!$G:$G,CAISO_Flex_SP!$B:$B,@INDIRECT("A"&ROW()),CAISO_Flex_SP!$F:$F,3)'
            caiso_cross_check['FilingSummary']['M{}'.format(row_number_summary)] = '=@INDIRECT("L"&ROW())-@INDIRECT("K"&ROW())'
            caiso_cross_check['FilingSummary']['M{}'.format(row_number_summary)].fill = PatternFill(start_color='FFF2CC',end_color='FFF2CC',fill_type='solid')
            caiso_cross_check['FilingSummary']['Y{}'.format(row_number_summary)] = '=@INDIRECT("L"&ROW())-@INDIRECT("K"&ROW())'

            # update log with compliance:
            requirements = caiso_cross_check['Requirements']['B{}'.format(row_number_summary+1)].value
            resources = caiso_cross_check['Requirements']['C{}'.format(row_number_summary+1)].value + \
                caiso_cross_check['Requirements']['D{}'.format(row_number_summary+1)].value
            if resources<=requirements:
                compliance = 'Compliant'
            else:
                compliance = 'Noncompliant'
            self.consolidation_logger.data.loc[(),'compliance'] = compliance
            row_number_summary += 1
            for _,row in physical_resources.loc[(physical_resources.loc[:,'organization_id']==organization_id),:].iterrows():
                ra_summary['PhysicalResources']['A{}'.format(row_number_physical_resources)] = organization_id
                ra_summary['PhysicalResources']['B{}'.format(row_number_physical_resources)] = row.loc['contract_id']
                ra_summary['PhysicalResources']['C{}'.format(row_number_physical_resources)] = row.loc['resource_id']
                ra_summary['PhysicalResources']['D{}'.format(row_number_physical_resources)].value = row.loc['resource_adequacy_system']
                ra_summary['PhysicalResources']['E{}'.format(row_number_physical_resources)].value = row.loc['resource_adequacy_local']
                ra_summary['PhysicalResources']['F{}'.format(row_number_physical_resources)].value = row.loc['resource_mcc_bucket']
                ra_summary['PhysicalResources']['G{}'.format(row_number_physical_resources)].value = row.loc['continuous_availability']
                ra_summary['PhysicalResources']['H{}'.format(row_number_physical_resources)].value = row.loc['resource_adequacy_committed_flexible']
                ra_summary['PhysicalResources']['I{}'.format(row_number_physical_resources)].value = row.loc['resource_adequacy_flexibility_category']
                ra_summary['PhysicalResources']['J{}'.format(row_number_physical_resources)] = '=VLOOKUP(@INDIRECT("C"&ROW()),\'NQC_List\'!$B:$D,2,FALSE)'
                ra_summary['PhysicalResources']['K{}'.format(row_number_physical_resources)] = '=VLOOKUP(@INDIRECT("C"&ROW()),\'NQC_List\'!$B:$D,3,FALSE)'
                caiso_cross_check['Filings']['A{}'.format(row_number_physical_resources+1)] = organization_id
                caiso_cross_check['Filings']['B{}'.format(row_number_physical_resources+1)] = row.loc['contract_id']
                caiso_cross_check['Filings']['C{}'.format(row_number_physical_resources+1)] = row.loc['resource_id']
                caiso_cross_check['Filings']['D{}'.format(row_number_physical_resources+1)] = '=CONCATENATE(@INDIRECT("A"&ROW()),@INDIRECT("C"&ROW()),@INDIRECT("G"&ROW()))'
                caiso_cross_check['Filings']['E{}'.format(row_number_physical_resources+1)] = '=CONCATENATE(@INDIRECT("A"&ROW()),@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['F{}'.format(row_number_physical_resources+1)] = '=CONCATENATE(@INDIRECT("A"&ROW()),@INDIRECT("C"&ROW()),@INDIRECT("M"&ROW()))'
                caiso_cross_check['Filings']['G{}'.format(row_number_physical_resources+1)].value = row.loc['resource_adequacy_system']
                caiso_cross_check['Filings']['H{}'.format(row_number_physical_resources+1)].value = row.loc['resource_adequacy_local']
                caiso_cross_check['Filings']['I{}'.format(row_number_physical_resources+1)] = '=SUMIFS($G:$G,$A:$A,@INDIRECT("A"&ROW()),$C:$C,@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['J{}'.format(row_number_physical_resources+1)] = '=SUMIFS(CAISO_Sys_SP!$I:$I,CAISO_Sys_SP!$C:$C,@INDIRECT("A"&ROW()),CAISO_Sys_SP!$F:$F,@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['K{}'.format(row_number_physical_resources+1)] = '=IF(@INDIRECT("J"&ROW())<>@INDIRECT("I"&ROW()),"Y: "&@INDIRECT("J"&ROW())-@INDIRECT("I"&ROW())&" MW","-")'
                caiso_cross_check['Filings']['L{}'.format(row_number_physical_resources+1)].value = row.loc['resource_mcc_bucket']
                caiso_cross_check['Filings']['M{}'.format(row_number_physical_resources+1)].value = row.loc['resource_adequacy_committed_flexible']
                caiso_cross_check['Filings']['N{}'.format(row_number_physical_resources+1)] = '=SUMIFS($M:$M,$A:$A,@INDIRECT("A"&ROW()),$C:$C,@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['O{}'.format(row_number_physical_resources+1)] = '=SUMIFS(CAISO_Flex_SP!$G:$G,CAISO_Flex_SP!$B:$B,@INDIRECT("A"&ROW()),CAISO_Flex_SP!$E:$E,@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['P{}'.format(row_number_physical_resources+1)] = '=IF(@INDIRECT("O"&ROW())<>@INDIRECT("N"&ROW()),"Y: "&@INDIRECT("O"&ROW())-@INDIRECT("N"&ROW())&" MW","-")'
                caiso_cross_check['Filings']['Q{}'.format(row_number_physical_resources+1)].value = row.loc['resource_adequacy_flexibility_category']
                caiso_cross_check['Filings']['R{}'.format(row_number_physical_resources+1)].value = get_zone(row.loc['resource_id'])
                caiso_cross_check['Filings']['S{}'.format(row_number_physical_resources+1)] = '=IF(NOT(ISBLANK(@INDIRECT("G"&ROW()))),IF(ISNA(VLOOKUP(@INDIRECT("A"&ROW()),CAISO_Sys_SP!$A:$A,1,FALSE)),"N","-"),"")'
                caiso_cross_check['Filings']['T{}'.format(row_number_physical_resources+1)] = '=IF(AND(NOT(ISBLANK(@INDIRECT("H"&ROW()))),NOT(@INDIRECT("H"&ROW())=0)),IF(ISNA(VLOOKUP(@INDIRECT("B"&ROW()),CAISO_Sys_SP!$B:$B,1,FALSE)),"N","-"),"")'
                caiso_cross_check['Filings']['U{}'.format(row_number_physical_resources+1)] = '=IF(NOT(ISBLANK(@INDIRECT("M"&ROW()))),IF(ISNA(VLOOKUP(@INDIRECT("C"&ROW()),CAISO_Flex_SP!$A:$A,1,FALSE)),"N","-"),"")'
                for col in 'DEFIJKNOPSTU':
                    caiso_cross_check['Filings']['{}{}'.format(col,row_number_physical_resources+1)].fill = PatternFill(start_color='DDEBF7',end_color='DDEBF7',fill_type='solid')
                row_number_physical_resources += 1
            for _,row in demand_response.loc[(demand_response.loc[:,'organization_id']==organization_id),:].iterrows():
                ra_summary['PhysicalResources']['A{}'.format(row_number_physical_resources)] = organization_id
                ra_summary['PhysicalResources']['B{}'.format(row_number_physical_resources)] = row.loc['contract_id']
                ra_summary['PhysicalResources']['C{}'.format(row_number_physical_resources)] = row.loc['program_id']
                ra_summary['PhysicalResources']['D{}'.format(row_number_physical_resources)].value = row.loc['resource_adequacy_system']
                ra_summary['PhysicalResources']['E{}'.format(row_number_physical_resources)].value = row.loc['resource_adequacy_local']
                ra_summary['PhysicalResources']['F{}'.format(row_number_physical_resources)].value = row.loc['resource_mcc_bucket']
                ra_summary['PhysicalResources']['G{}'.format(row_number_physical_resources)].value = 0
                ra_summary['PhysicalResources']['H{}'.format(row_number_physical_resources)].value = row.loc['resource_adequacy_committed_flexible']
                ra_summary['PhysicalResources']['I{}'.format(row_number_physical_resources)].value = row.loc['resource_adequacy_flexibility_category']
                ra_summary['PhysicalResources']['J{}'.format(row_number_physical_resources)] = '=VLOOKUP(@INDIRECT("C"&ROW()),\'NQC_List\'!$B:$D,2,FALSE)'
                ra_summary['PhysicalResources']['K{}'.format(row_number_physical_resources)] = '=VLOOKUP(@INDIRECT("C"&ROW()),\'NQC_List\'!$B:$D,3,FALSE)'
                caiso_cross_check['Filings']['A{}'.format(row_number_physical_resources+1)] = organization_id
                caiso_cross_check['Filings']['B{}'.format(row_number_physical_resources+1)] = row.loc['contract_id']
                caiso_cross_check['Filings']['C{}'.format(row_number_physical_resources+1)] = row.loc['program_id']
                caiso_cross_check['Filings']['D{}'.format(row_number_physical_resources+1)] = '=CONCATENATE(@INDIRECT("A"&ROW()),@INDIRECT("C"&ROW()),@INDIRECT("G"&ROW()))'
                caiso_cross_check['Filings']['E{}'.format(row_number_physical_resources+1)] = '=CONCATENATE(@INDIRECT("A"&ROW()),@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['F{}'.format(row_number_physical_resources+1)] = '=CONCATENATE(@INDIRECT("A"&ROW()),@INDIRECT("C"&ROW()),@INDIRECT("M"&ROW()))'
                caiso_cross_check['Filings']['G{}'.format(row_number_physical_resources+1)].value = row.loc['resource_adequacy_system']
                caiso_cross_check['Filings']['H{}'.format(row_number_physical_resources+1)].value = row.loc['resource_adequacy_local']
                caiso_cross_check['Filings']['I{}'.format(row_number_physical_resources+1)] = '=SUMIFS($G:$G,$A:$A,@INDIRECT("A"&ROW()),$C:$C,@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['J{}'.format(row_number_physical_resources+1)] = '=SUMIFS(CAISO_Sys_SP!$I:$I,CAISO_Sys_SP!$C:$C,@INDIRECT("A"&ROW()),CAISO_Sys_SP!$F:$F,@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['K{}'.format(row_number_physical_resources+1)] = '=IF(@INDIRECT("J"&ROW())<>@INDIRECT("I"&ROW()),"Y: "&@INDIRECT("J"&ROW())-@INDIRECT("I"&ROW())&" MW","-")'
                caiso_cross_check['Filings']['L{}'.format(row_number_physical_resources+1)].value = row.loc['resource_mcc_bucket']
                caiso_cross_check['Filings']['M{}'.format(row_number_physical_resources+1)].value = row.loc['resource_adequacy_committed_flexible']
                caiso_cross_check['Filings']['N{}'.format(row_number_physical_resources+1)] = '=SUMIFS($M:$M,$A:$A,@INDIRECT("A"&ROW()),$C:$C,@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['O{}'.format(row_number_physical_resources+1)] = '=SUMIFS(CAISO_Flex_SP!$G:$G,CAISO_Flex_SP!$B:$B,@INDIRECT("A"&ROW()),CAISO_Flex_SP!$E:$E,@INDIRECT("C"&ROW()))'
                caiso_cross_check['Filings']['P{}'.format(row_number_physical_resources+1)] = '=IF(@INDIRECT("O"&ROW())<>@INDIRECT("N"&ROW()),"Y: "&@INDIRECT("O"&ROW())-@INDIRECT("N"&ROW())&" MW","-")'
                caiso_cross_check['Filings']['Q{}'.format(row_number_physical_resources+1)].value = row.loc['resource_adequacy_flexibility_category']
                caiso_cross_check['Filings']['R{}'.format(row_number_physical_resources+1)].value = get_zone(row.loc['program_id'])
                caiso_cross_check['Filings']['S{}'.format(row_number_physical_resources+1)] = '=IF(NOT(ISBLANK(@INDIRECT("G"&ROW()))),IF(ISNA(VLOOKUP(@INDIRECT("A"&ROW()),CAISO_Sys_SP!$A:$A,1,FALSE)),"N","-"),"")'
                caiso_cross_check['Filings']['T{}'.format(row_number_physical_resources+1)] = '=IF(AND(NOT(ISBLANK(@INDIRECT("H"&ROW()))),NOT(@INDIRECT("H"&ROW())=0)),IF(ISNA(VLOOKUP(@INDIRECT("B"&ROW()),CAISO_Sys_SP!$B:$B,1,FALSE)),"N","-"),"")'
                caiso_cross_check['Filings']['U{}'.format(row_number_physical_resources+1)] = '=IF(NOT(ISBLANK(@INDIRECT("M"&ROW()))),IF(ISNA(VLOOKUP(@INDIRECT("C"&ROW()),CAISO_Flex_SP!$A:$A,1,FALSE)),"N","-"),"")'
                for col in 'DEFIJKNOPSTU':
                    caiso_cross_check['Filings']['{}{}'.format(col,row_number_physical_resources+1)].fill = PatternFill(start_color='DDEBF7',end_color='DDEBF7',fill_type='solid')
                row_number_physical_resources += 1

        # apply conditional formatting to flex-rar sheet:
        ra_summary['FlexRAR'].conditional_formatting.add(
            'C{}:C{}'.format(first_row_number_summary,row_number_summary-1),
            CellIsRule(
                operator='lessThan',
                formula=['@INDIRECT("B"&ROW())'],
                stopIfTrue=False,
                fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                font=Font(color='9C0006')
            )
        )
        ra_summary['FlexRAR'].conditional_formatting.add(
            'D{}:D{}'.format(first_row_number_summary,row_number_summary-1),
            CellIsRule(
                operator='lessThan',
                formula=[1.0],
                stopIfTrue=False,
                fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                font=Font(color='9C0006')
            )
        )
        ra_summary['FlexRAR'].conditional_formatting.add(
            'F{}:F{}'.format(first_row_number_summary,row_number_summary-1),
            CellIsRule(
                operator='lessThan',
                formula=['@INDIRECT("E"&ROW())'],
                stopIfTrue=False,
                fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                font=Font(color='9C0006')
            )
        )

        # create totals row in MCC_Check worksheet:
        ra_summary['MCC_Check']['A{}'.format(row_number_summary)] = 'Total:'
        for col in 'BCDEFGHIJKLMNOP':
            ra_summary['MCC_Check']['{}{}'.format(col,row_number_summary)] = '=SUM({0}{1}:{0}{2})'.format(col,first_row_number_summary,row_number_summary-1)

        # create totals row in FlexRAR worksheet:
        ra_summary['FlexRAR']['A{}'.format(row_number_summary)] = 'Total:'
        ra_summary['FlexRAR']['D{}'.format(row_number_summary)] = '=IFERROR(INDIRECT("C"&ROW())/@INDIRECT("B"&ROW()),0)'
        for col in 'BCEFGHIJLMNOQRST':
            ra_summary['FlexRAR']['{}{}'.format(col,row_number_summary)] = '=SUM({0}6:{0}{1})'.format(col,row_number_summary-1)
        for col in 'ABCDEFGHIJKLMNOPQRST':
            ra_summary['FlexRAR']['{}{}'.format(col,row_number_summary)].fill = PatternFill(start_color='00FF00',end_color='00FF00',fill_type='solid')

        # apply conditional formatting to local true-up sheet:
        for col in ['F','K','P','U','Z','AG','AJ','AO','AT','AY','BD','BI','BN','BS']:
            ra_summary['LocalTrueUp'].conditional_formatting.add(
                '{0}{1}:{0}{2}'.format(col,first_row_number_summary,row_number_summary-1),
                CellIsRule(
                    operator='lessThan',
                    formula=[0],
                    stopIfTrue=False,
                    fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                    font=Font(color='9C0006')
                )
            )

        # apply conditional formatting to certifying officer sheet:
        ra_summary['CertifyingOfficers'].conditional_formatting.add(
            'E{}:E{}'.format(first_row_number_summary,row_number_summary-1),
            CellIsRule(
                operator='equal',
                formula=['"Yes"'],
                stopIfTrue=False,
                fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                font=Font(color='9C0006')
            )
        )
        ra_summary['CertifyingOfficers'].conditional_formatting.add(
            'G{}:G{}'.format(first_row_number_summary,row_number_summary-1),
            CellIsRule(
                operator='equal',
                formula=['"Yes"'],
                stopIfTrue=False,
                fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                font=Font(color='9C0006')
            )
        )

        # write total lines in np and sp worksheets:
        for sheet_name in ['NP26','SP26']:
            ra_summary[sheet_name]['A{}'.format(row_number_summary)] = 'Total:'
            ra_summary[sheet_name]['A{}'.format(row_number_summary)].fill = PatternFill(start_color='00FF00',end_color='00FF00',fill_type='solid')
            for col in 'BCDEFGHI':
                ra_summary[sheet_name]['{}{}'.format(col,row_number_summary)] = '=SUM({0}{1}:{0}{2})'.format(col,first_row_number_summary,row_number_summary-1)
                ra_summary[sheet_name]['{}{}'.format(col,row_number_summary)].fill = PatternFill(start_color='00FF00',end_color='00FF00',fill_type='solid')
            ra_summary[sheet_name]['I{}'.format(row_number_summary)] = '=@INDIRECT("C"&ROW())+@INDIRECT("E"&ROW())'
            ra_summary[sheet_name]['A{}'.format(row_number_summary+1)].value = 'Net Long/Short:'
            ra_summary[sheet_name]['C{}'.format(row_number_summary+1)].value = 'Excess in Zone:'
            ra_summary[sheet_name]['A{}'.format(row_number_summary+1)].fill = PatternFill(start_color='CCFFCC',end_color='CCFFCC',fill_type='solid')
            ra_summary[sheet_name]['E{}'.format(row_number_summary+1)] = '=@INDIRECT("D"&ROW()-1)+@INDIRECT("C"&ROW()-1)-@INDIRECT("B"&ROW()-1)'

            # apply conditional formatting to columns:
            ra_summary[sheet_name].conditional_formatting.add(
                'F{}:F{}'.format(first_row_number_summary,row_number_summary-1),
                CellIsRule(
                    operator='greaterThan',
                    formula=[0],
                    stopIfTrue=False,
                    fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                    font=Font(color='9C0006')
                )
            )
            ra_summary[sheet_name].conditional_formatting.add(
                '{0}{1}:{0}{2}'.format({'NP26':'M','SP26':'L'}[sheet_name],first_row_number_summary,row_number_summary-1),
                CellIsRule(
                    operator='equal',
                    formula=['"Monthly Filing Not Found"'],
                    stopIfTrue=False,
                    fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                    font=Font(color='9C0006')
                )
            )

        # apply conditional formatting to requirements sheet in caiso supply plan cross-check file:
        caiso_cross_check['Requirements'].conditional_formatting.add(
            'I{}:I{}'.format(first_row_number_summary+1,row_number_summary),
            CellIsRule(
                operator='lessThan',
                formula=['@INDIRECT("H"&ROW())'],
                stopIfTrue=False,
                fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                font=Font(color='9C0006')
            )
        )
        caiso_cross_check['Requirements'].conditional_formatting.add(
            'J{}:J{}'.format(first_row_number_summary+1,row_number_summary),
            CellIsRule(
                operator='lessThan',
                formula=[1.0],
                stopIfTrue=False,
                fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                font=Font(color='9C0006')
            )
        )
        caiso_cross_check['Requirements'].conditional_formatting.add(
            'L{}:L{}'.format(first_row_number_summary+1,row_number_summary),
            CellIsRule(
                operator='lessThan',
                formula=['@INDIRECT("K"&ROW())'],
                stopIfTrue=False,
                fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                font=Font(color='9C0006')
            )
        )
        for col in ['Z','AE','AJ','AO','AT','AY','BD','BI','BN','BS','BX','CC','CH','CM']:
            caiso_cross_check['Requirements'].conditional_formatting.add(
                '{0}{1}:{0}{2}'.format(col,first_row_number_summary+1,row_number_summary),
                CellIsRule(
                    operator='lessThan',
                    formula=[0],
                    stopIfTrue=False,
                    fill=PatternFill(start_color='FFC7CE',end_color='FFC7CE',fill_type='solid'),
                    font=Font(color='9C0006')
                )
            )

        ra_summary.save(str(self.paths.get_path('ra_summary')))
        ra_summary.close()

        caiso_cross_check.save(str(self.paths.get_path('caiso_cross_check')))
        caiso_cross_check.close()

        # check time and report:
        run_time = (ts.now() - init_time).total_seconds()
        self.logger.log('Retrieved LSE Filings in {:02.0f}:{:02.0f}:{:05.2f}'.format(int(run_time/3600),int((run_time%3600)/60),run_time%60),'INFORMATION')

        # return all dataframes for testing:
        return {
            'summary' : summary,
            'physical_resources' : physical_resources,
            'demand_response' : demand_response,
        }

    # consolidate caiso supply plans into cross-check file:
    def consolidate_supply_plans(self):
        # start timer:
        init_time = ts.now()
        self.logger.log('Copying CAISO Supply Plan into Cross-Check File','INFORMATION')

        # open caiso supply plan cross-check file:
        path = self.paths.get_path('caiso_cross_check')
        caiso_cross_check = open_workbook(path,data_only=False,read_only=False)

        # retrieve caiso system supply plan table (.xls format):
        columns = [
            'validation_status',
            'supplier',
            'resource_id',
            'local_resource_adequacy',
            'system_resource_adequacy',
            'total_capacity',
            'start_date',
            'end_date',
            'organization_id_caiso',
            'errors_and_warnings',
        ]
        supply_plan_system = pd.DataFrame(columns=columns)
        path = self.paths.get_path('supply_plan_system')
        workbook = xlrd.open_workbook(path)
        sheet = workbook.sheet_by_index(0)
        for row_number in range(1,sheet.nrows):
            new_row = [sheet.cell_value(rowx=row_number,colx=column_number) for column_number in range(sheet.ncols)]
            supply_plan_system.loc[len(supply_plan_system)] = new_row
        # copy supply plan data into cross-check file:
        row_number = 2
        for _,row in supply_plan_system.iterrows():
            caiso_cross_check['CAISO_Sys_SP']['A{}'.format(row_number)] = '=CONCATENATE(INDIRECT("C"&ROW()),INDIRECT("F"&ROW()),INDIRECT("I"&ROW()))'
            caiso_cross_check['CAISO_Sys_SP']['B{}'.format(row_number)] = '=CONCATENATE(INDIRECT("C"&ROW()),INDIRECT("F"&ROW()))'
            caiso_cross_check['CAISO_Sys_SP']['C{}'.format(row_number)] = '=VLOOKUP(INDIRECT("N"&ROW()),LoadServingEntities!B:C,2,FALSE)'
            caiso_cross_check['CAISO_Sys_SP']['J{}'.format(row_number)] = '=SUMIFS(I:I,C:C,INDIRECT("C"&ROW()),F:F,INDIRECT("F"&ROW()))'
            caiso_cross_check['CAISO_Sys_SP']['K{}'.format(row_number)] = '=SUMIFS(G:G,C:C,INDIRECT("C"&ROW()),F:F,INDIRECT("F"&ROW()))'
            caiso_cross_check['CAISO_Sys_SP']['P{}'.format(row_number)] = '=IF(ISNA(VLOOKUP(INDIRECT("A"&ROW()),Filings!D:D,1,FALSE)),"N","-")'
            for column_letter in 'ABCJKP':
                caiso_cross_check['CAISO_Sys_SP']['{}{}'.format(column_letter,row_number)].fill = PatternFill(start_color='DDEBF7',end_color='DDEBF7',fill_type='solid')
            column_index = 0
            for column in columns:
                caiso_cross_check['CAISO_Sys_SP']['{}{}'.format('DEFGHILMNO'[column_index],row_number)].value = row.loc[column]
                column_index += 1
            row_number += 1

        # retrieve caiso flex supply plan table (old .xls format):
        columns = [
            'validation_status',
            'supplier',
            'resource_id',
            'category',
            'flex_capacity',
            'start_date',
            'end_date',
            'organization_id_caiso',
            'errors_and_warnings',
        ]
        supply_plan_local = pd.DataFrame(columns=columns)
        path = self.paths.get_path('supply_plan_local')
        workbook = xlrd.open_workbook(path)
        sheet = workbook.sheet_by_index(0)
        for row_number in range(1,sheet.nrows):
            new_row = [sheet.cell_value(rowx=row_number,colx=column_number) for column_number in range(sheet.ncols)]
            supply_plan_local.loc[len(supply_plan_local)] = new_row
        row_number = 2
        # copy supply plan data into cross-check file:
        for _,row in supply_plan_local.iterrows():
            caiso_cross_check['CAISO_Flex_SP']['A{}'.format(row_number)] = '=CONCATENATE(INDIRECT("B"&ROW()),INDIRECT("E"&ROW()),INDIRECT("G"&ROW()))'
            caiso_cross_check['CAISO_Flex_SP']['B{}'.format(row_number)] = '=VLOOKUP(INDIRECT("K"&ROW()),LoadServingEntities!B:C,2,FALSE)'
            caiso_cross_check['CAISO_Flex_SP']['H{}'.format(row_number)] = '=SUMIFS(G:G,B:B,INDIRECT("B"&ROW()),E:E,INDIRECT("E"&ROW()),F:F,INDIRECT("F"&ROW()))'
            caiso_cross_check['CAISO_Flex_SP']['M{}'.format(row_number)] = '=IF(ISNA(VLOOKUP(INDIRECT("A"&ROW()),Filings!F:F,1,FALSE)),"N","-")'
            for column_letter in 'ABHM':
                caiso_cross_check['CAISO_Flex_SP']['{}{}'.format(column_letter,row_number)].fill = PatternFill(start_color='DDEBF7',end_color='DDEBF7',fill_type='solid')
            column_index = 0
            for column in columns:
                caiso_cross_check['CAISO_Flex_SP']['{}{}'.format('CDEFGIJKL'[column_index],row_number)].value = row.loc[column]
                column_index += 1
            row_number += 1

        # save and close:
        caiso_cross_check.save(str(self.paths.get_path('caiso_cross_check')))
        caiso_cross_check.close()

        # check time and report:
        run_time = (ts.now() - init_time).total_seconds()
        self.logger.log('Copied CAISO Supply Plan to Cross-Check File in {:02.0f}:{:02.0f}:{:05.2f}'.format(int(run_time/3600),int((run_time%3600)/60),run_time%60),'INFORMATION')

        return [supply_plan_system,supply_plan_local]

    def check_files(self):
        '''
        Checks whether all files required for consolidation are available and provides a table of results

        returns:
            boolean - true if all files are available, false otherwise
        '''
        self.attachment_logger.load_log()
        def set_file_status(ra_category,organization_id):
            attachments = self.attachment_logger.data
            filing_month = pd.to_datetime(self.configuration_options.get_option('filing_month'))
            if ra_category in ('ra_monthly_filing','supply_plan_system','supply_plan_local'):
                effective_date = filing_month
            else:
                effective_date = filing_month.replace(month=1)
            versions = attachments.loc[
                (attachments.loc[:,'ra_category']==ra_category) & \
                (attachments.loc[:,'effective_date']==effective_date) & \
                (attachments.loc[:,'organization_id']==organization_id), \
                ['attachment_id','archive_path']
            ]
            versions.sort_values('archive_path',ascending=False,inplace=True)
            file_information = pd.Series({
                'filing_month' : filing_month,
                'ra_category' : ra_category,
                'effective_date' : effective_date,
                'organization_id' : organization_id,
                'attachment_id' : '',
                'archive_path' : '',
                'status' : '',
                'compliance' : '' if ra_category=='ra_monthly_filing' else 'n/a',
            })
            if len(versions)>0 and Path(versions.iloc[0].loc['archive_path']).is_file():
                file_information.loc['attachment_id'] = versions.iloc[0].loc['attachment_id']
                file_information.loc['archive_path'] = versions.iloc[0].loc['archive_path']
                file_information.loc['status'] = 'Ready'
            elif ra_category=='incremental_local' and filing_month.month<=6:
                file_information.loc['attachment_id'] = ''
                file_information.loc['archive_path'] = ''
                file_information.loc['status'] = 'Not Required'
            elif ra_category=='ra_monthly_filing':
                if len(versions)>0:
                    file_information.loc['attachment_id'] = versions.iloc[0].loc['attachment_id']
                    file_information.loc['archive_path'] = ''
                    file_information.loc['status'] = 'Invalid File'
                else:
                    file_information.loc['attachment_id'] = ''
                    file_information.loc['archive_path'] = ''
                    file_information.loc['status'] = 'File Not Submitted'
            elif self.paths.get_path(ra_category) is not None:
                if self.paths.get_path(ra_category).is_file():
                    file_information.loc['attachment_id'] = ''
                    file_information.loc['archive_path'] = str(self.paths.get_path(ra_category))
                    file_information.loc['status'] = 'Ready'
                else:
                    file_information.loc['attachment_id'] = ''
                    file_information.loc['archive_path'] = ''
                    file_information.loc['status'] = 'File Not Found'
            else:
                file_information.loc['attachment_id'] = ''
                file_information.loc['archive_path'] = ''
                file_information.loc['status'] = 'File Not Found'
            # overwrite previous entries for a given file:
            check_columns = ['ra_category','effective_date','organization_id']
            previous_log_indices = (self.consolidation_logger.data.loc[:,check_columns]==file_information.loc[check_columns]).apply(all,axis='columns',result_type='reduce')
            if previous_log_indices.sum()>0:
                self.consolidation_logger.data.drop(index=self.consolidation_logger.data.loc[previous_log_indices,:].index,inplace=True)
            else:
                pass
            self.consolidation_logger.log(file_information)
        # get list of current lses from summary template file:
        path = self.paths.get_path('ra_summary_template')
        ra_summary = open_workbook(path,data_only=True,read_only=True)
        data_range = get_data_range(ra_summary['Summary'],'A','',self.organizations)
        active_lses = [row[0].value for row in data_range]
        ra_categories = ['year_ahead','incremental_local','month_ahead','cam_rmr','supply_plan_system','supply_plan_local'] + ['ra_monthly_filing'] * len(active_lses)
        organization_ids = ['CEC','CEC','CPUC','CPUC','CAISO','CAISO'] + active_lses
        # get list of active load serving entities from summary sheet:
        for ra_category,organization_id in zip(ra_categories,organization_ids):
            set_file_status(ra_category,organization_id)
        ready =  all([s in ('Ready','Not Required') for s in self.consolidation_logger.data.loc[(self.consolidation_logger.data.loc[:,'ra_category']!='ra_monthly_filing'),'status']])
        self.consolidation_logger.commit()
        return ready