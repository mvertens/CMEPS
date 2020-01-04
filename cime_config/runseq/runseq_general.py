#!/usr/bin/env python

import os, shutil, sys
from CIME.utils import expect
from gen_runseq import RunSeq
from driver_config import DriverConfig

_CIMEROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir,os.pardir,os.pardir,os.pardir))
sys.path.append(os.path.join(_CIMEROOT, "scripts", "Tools"))

from standard_script_setup import *

#pylint:disable=undefined-variable
logger = logging.getLogger(__name__)

def runseq(case, coupling_times):

    rundir         = case.get_value("RUNDIR")
    caseroot       = case.get_value("CASEROOT")
    cpl_seq_option = case.get_value('CPL_SEQ_OPTION')

    driver_config = DriverConfig(case, coupling_times)
    run_atm, atm_to_med, med_to_atm, atm_cpl_time = driver_config['atm']
    run_ice, ice_to_med, med_to_ice, ice_cpl_time = driver_config['ice']
    run_glc, glc_to_med, med_to_glc, glc_cpl_time = driver_config['glc']
    run_lnd, lnd_to_med, med_to_lnd, lnd_cpl_time = driver_config['lnd']
    run_ocn, ocn_to_med, med_to_ocn, ocn_cpl_time = driver_config['ocn']  
    run_rof, rof_to_med, med_to_rof, rof_cpl_time = driver_config['rof']
    run_wav, wav_to_med, med_to_wav, wav_cpl_time = driver_config['wav']

    # Note: assume that atm_cpl_dt, lnd_cpl_dt, ice_cpl_dt and wav_cpl_dt are the same

    with RunSeq(os.path.join(caseroot, "CaseDocs", "nuopc.runseq")) as runseq:

        #------------------
        runseq.enter_time_loop(glc_cpl_time, newtime=(glc_to_med or med_to_glc), active=med_to_glc)
        #------------------

        runseq.add_action("MED med_phases_prep_glc_avg"    , med_to_glc)
        runseq.add_action("MED -> GLC :remapMethod=redist" , med_to_glc)
        runseq.add_action("GLC"                            , run_glc)
        runseq.add_action("GLC -> MED :remapMethod=redist" , glc_to_med)

        #------------------
        runseq.enter_time_loop(rof_cpl_time, newtime=rof_to_med)
        #------------------

        runseq.add_action("MED med_phases_prep_rof_avg"    , med_to_rof)
        runseq.add_action("MED -> ROF :remapMethod=redist" , med_to_rof)
        runseq.add_action("ROF"                            , run_rof and atm_cpl_time < rof_cpl_time)

        #------------------
        runseq.enter_time_loop(ocn_cpl_time, newtime=((ocn_cpl_time < rof_cpl_time) or (not rof_to_med)))
        #------------------
        
        if (cpl_seq_option == 'RASM'):
            runseq.add_action("MED med_phases_prep_ocn_accum_avg"  , med_to_ocn and atm_cpl_time < ocn_cpl_time)
            runseq.add_action("MED -> OCN :remapMethod=redist"     , med_to_ocn and atm_cpl_time < ocn_cpl_time)
            runseq.add_action("OCN"                                , run_ocn    and atm_cpl_time < ocn_cpl_time)
            runseq.add_action("OCN -> MED :remapMethod=redist"     , ocn_to_med and atm_cpl_time < ocn_cpl_time)
        else:
            runseq.add_action("MED med_phases_prep_ocn_accum_avg"  , med_to_ocn and atm_cpl_time == ocn_cpl_time)
            runseq.add_action("MED -> OCN :remapMethod=redist"     , med_to_ocn and atm_cpl_time == ocn_cpl_time)

        #------------------
        runseq.enter_time_loop(atm_cpl_time, newtime=((atm_cpl_time < ocn_cpl_time)))
        #------------------

        if (cpl_seq_option == 'RASM'):
            runseq.add_action("MED med_phases_prep_ocn_accum_avg"  , med_to_ocn and atm_cpl_time == ocn_cpl_time)
            runseq.add_action("MED -> OCN :remapMethod=redist"     , med_to_ocn and atm_cpl_time == ocn_cpl_time)
            runseq.add_action("OCN"                                , run_ocn)
            runseq.add_action("OCN -> MED :remapMethod=redist"     , ocn_to_med)
            runseq.add_action("MED med_phases_prep_ocn_map"        , med_to_ocn)
            runseq.add_action("MED med_phases_aofluxes_run"        , run_ocn and run_atm and (med_to_ocn or med_to_atm))
            runseq.add_action("MED med_phases_prep_ocn_merge"      , med_to_ocn)
            runseq.add_action("MED med_phases_prep_ocn_accum_fast" , med_to_ocn)
            runseq.add_action("MED med_phases_ocnalb_run"          , run_ocn and run_atm and (med_to_ocn or med_to_atm))

        runseq.add_action("MED med_phases_prep_lnd"                , med_to_lnd)
        runseq.add_action("MED -> LND :remapMethod=redist"         , med_to_lnd)

        runseq.add_action("MED med_phases_prep_ice"                , ice_to_med)
        runseq.add_action("MED -> ICE :remapMethod=redist"         , ice_to_med)

        runseq.add_action("ICE"                                    , run_ice)
        runseq.add_action("ROF"                                    , run_rof and atm_cpl_time == rof_cpl_time)
        runseq.add_action("LND"                                    , run_lnd)

        runseq.add_action("WAV"                                    , run_wav )
        runseq.add_action("WAV -> MED :remapMethod=redist"         , wav_to_med)

        if (cpl_seq_option == 'TIGHT'):
            runseq.add_action("OCN"                                , run_ocn)
            runseq.add_action("OCN -> MED :remapMethod=redist"     , ocn_to_med)

        runseq.add_action("ICE -> MED :remapMethod=redist"         , ice_to_med)
        runseq.add_action("MED med_fraction_set"                   , ice_to_med)

        if (cpl_seq_option == 'TIGHT'):
            runseq.add_action("MED med_phases_prep_ocn_map"        , med_to_ocn)
            runseq.add_action("MED med_phases_aofluxes_run"        , ocn_to_med and atm_to_med)
            runseq.add_action("MED med_phases_prep_ocn_merge"      , med_to_ocn)
            runseq.add_action("MED med_phases_prep_ocn_accum_fast" , med_to_ocn)
            runseq.add_action("MED med_phases_ocnalb_run"          , ocn_to_med and atm_to_med)

        runseq.add_action("LND -> MED :remapMethod=redist"         , lnd_to_med)
        runseq.add_action("MED med_phases_prep_rof_accum"          , med_to_rof)
        runseq.add_action("MED med_phases_prep_glc_accum"          , med_to_glc)

        runseq.add_action("MED med_phases_prep_atm"                , med_to_atm)
        runseq.add_action("MED -> ATM :remapMethod=redist"         , med_to_atm)
        runseq.add_action("ATM"                                    , run_atm)
        runseq.add_action("ATM -> MED :remapMethod=redist"         , atm_to_med)

        #runseq.add_action("MED med_phases_history_write"          , rof_to_med)

        #------------------
        runseq.leave_time_loop(rof_to_med and (atm_cpl_time < rof_cpl_time))
        #------------------

        runseq.add_action("ROF -> MED :remapMethod=redist" , rof_to_med)
        runseq.add_action("MED med_phases_history_write"   , True)

        #------------------
        runseq.leave_time_loop(rof_to_med and (atm_cpl_time < rof_cpl_time))
        #------------------

    shutil.copy(os.path.join(caseroot, "CaseDocs", "nuopc.runseq"), rundir)
