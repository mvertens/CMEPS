module med_phases_history_mod

  !-----------------------------------------------------------------------------
  ! Mediator History control
  !
  ! Each time loop has its own associated clock object. NUOPC manages
  ! these clock objects, i.e. their creation and destruction, as well as
  ! startTime, endTime, timeStep adjustments during the execution. The
  ! outer most time loop of the run sequence is a special case. It uses
  ! the driver clock itself. If a single outer most loop is defined in
  ! the run sequence provided by freeFormat, this loop becomes the driver
  ! loop level directly. Therefore, setting the timeStep or runDuration
  ! for the outer most time loop results modifiying the driver clock
  ! itself. However, for cases with concatenated loops on the upper level
  ! of the run sequence in freeFormat, a single outer loop is added
  ! automatically during ingestion, and the driver clock is used for this
  ! loop instead.
  !-----------------------------------------------------------------------------

  use med_kind_mod          , only : CX=>SHR_KIND_CX, CS=>SHR_KIND_CS, CL=>SHR_KIND_CL, R8=>SHR_KIND_R8
  use ESMF                  , only : ESMF_GridComp, ESMF_GridCompGet
  use ESMF                  , only : ESMF_VM, ESMF_VMGet
  use ESMF                  , only : ESMF_Clock, ESMF_ClockGet, ESMF_ClockSet, ESMF_ClockAdvance
  use ESMF                  , only : ESMF_ClockGetNextTime, ESMF_ClockGetAlarm
  use ESMF                  , only : ESMF_Calendar
  use ESMF                  , only : ESMF_Time, ESMF_TimeGet
  use ESMF                  , only : ESMF_TimeInterval, ESMF_TimeIntervalGet, ESMF_TimeIntervalSet
  use ESMF                  , only : ESMF_Alarm, ESMF_AlarmCreate, ESMF_AlarmSet
  use ESMF                  , only : ESMF_AlarmIsRinging, ESMF_AlarmRingerOff, ESMF_AlarmGet
  use ESMF                  , only : ESMF_FieldBundle, ESMF_FieldBundleGet
  use ESMF                  , only : ESMF_FieldBundleIsCreated, ESMF_FieldBundleRemove
  use ESMF                  , only : ESMF_LogWrite, ESMF_LOGMSG_INFO, ESMF_LOGMSG_ERROR, ESMF_LogFoundError
  use ESMF                  , only : ESMF_SUCCESS, ESMF_FAILURE, ESMF_MAXSTR, ESMF_LOGERR_PASSTHRU, ESMF_END_ABORT
  use ESMF                  , only : ESMF_Finalize
  use ESMF                  , only : operator(==), operator(-), operator(+), operator(/=), operator(<=)
  use NUOPC                 , only : NUOPC_CompAttributeGet
  use NUOPC_Model           , only : NUOPC_ModelGet
  use esmFlds               , only : compatm, complnd, compocn, compice, comprof, compglc, compwav, ncomps, compname
  use esmFlds               , only : fldListFr, fldListTo
  use med_constants_mod     , only : SecPerDay       => med_constants_SecPerDay
  use med_constants_mod     , only : czero           => med_constants_czero
  use med_utils_mod         , only : chkerr          => med_utils_ChkErr
  use med_methods_mod       , only : FB_reset        => med_methods_FB_reset
  use med_methods_mod       , only : FB_diagnose     => med_methods_FB_diagnose
  use med_methods_mod       , only : FB_GetFldPtr    => med_methods_FB_GetFldPtr
  use med_methods_mod       , only : FB_init         => med_methods_FB_init
  use med_methods_mod       , only : FB_accum        => med_methods_FB_accum
  use med_methods_mod       , only : FB_average      => med_methods_FB_average
  use med_methods_mod       , only : FB_fldchk       => med_methods_FB_fldchk
  use med_internalstate_mod , only : InternalState, mastertask, logunit
  use med_time_mod          , only : med_time_alarmInit
  use med_io_mod            , only : med_io_write, med_io_wopen, med_io_enddef
  use med_io_mod            , only : med_io_close, med_io_date2yyyymmdd, med_io_sec2hms
  use med_io_mod            , only : med_io_ymd2date
  use perf_mod              , only : t_startf, t_stopf

  implicit none
  private

  public :: med_phases_history_init
  public :: med_phases_history_write
  public :: med_phases_history_write_med
  public :: med_phases_history_write_atm
  public :: med_phases_history_write_ice
  public :: med_phases_history_write_glc
  public :: med_phases_history_write_lnd
  public :: med_phases_history_write_ocn
  public :: med_phases_history_write_rof
  public :: med_phases_history_write_wav

  private :: med_phases_history_write_hfile
  private :: med_phases_history_write_hfileaux
  private :: med_phases_history_get_filename
  private :: med_phases_history_get_auxflds
  private :: med_phases_history_output_alarminfo
  private :: med_phases_history_ymds2rday_offset

  type, public :: avgfile_type
     type(ESMF_FieldBundle) :: FBaccum    ! field bundle for time averaging
     integer                :: accumcnt   ! field bundle accumulation counter
  end type avgfile_type
  type(avgfile_type) :: avgfiles_import(ncomps)
  type(avgfile_type) :: avgfiles_export(ncomps)
  type(avgfile_type) :: avgfiles_aoflux_ocn
  type(avgfile_type) :: avgfiles_ocnalb_ocn
  type(avgfile_type) :: avgfiles_aoflux_atm
  type(avgfile_type) :: avgfiles_ocnalb_atm

  integer, parameter :: max_auxfiles = 10
  integer            :: num_auxfiles(ncomps) = 0
  type, public :: auxfile_type
     character(CS)              :: auxname       ! name for history file creation
     character(CL)              :: histfile = '' ! current history file name
     character(CS), allocatable :: flds(:)       ! array of aux field names
     character(CS)              :: alarmname     ! name of write alarm
     integer                    :: deltat        ! interval to write out aux data in seconds
     integer                    :: ntperfile     ! maximum number of time samples per file
     integer                    :: nt = 0        ! time in file
     logical                    :: useavg        ! if true, time average, otherwise instantaneous
     type(ESMF_FieldBundle)     :: FBaccum       ! field bundle for time averaging
     integer                    :: accumcnt      ! field bundle accumulation counter
  end type auxfile_type
  type(auxfile_type) :: auxfiles(ncomps, max_auxfiles)

  character(CL) :: case_name  ! case name
  character(CS) :: inst_tag   ! instance tag

  logical :: debug_alarms = .true.

  character(*), parameter :: u_FILE_u  = &
       __FILE__

!===============================================================================
contains
!===============================================================================

  subroutine med_phases_history_init(gcomp, rc)

    ! --------------------------------------
    ! Initialize mediator history file alarms
    ! This is called from med.F90 and is not a phase
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    type(InternalState)         :: is_local
    type(ESMF_Alarm)            :: alarm
    type(ESMF_TimeInterval)     :: alarmInterval ! alarm interval
    type(ESMF_Time)             :: nextAlarm     ! next restart alarm time
    type(ESMF_VM)               :: vm
    type(ESMF_Clock)            :: mclock
    type(ESMF_TimeInterval)     :: mtimestep
    type(ESMF_Time)             :: mCurrTime
    type(ESMF_Time)             :: mStartTime
    type(ESMF_TimeInterval)     :: timestep
    integer                     :: timestep_length
    character(CS)               :: alarmname     ! alarm name
    character(CL)               :: cvalue        ! attribute string
    character(CL)               :: hist_option   ! freq_option setting (ndays, nsteps, etc)
    integer                     :: hist_n        ! freq_n setting relative to freq_option
    logical                     :: isPresent
    logical                     :: isSet
    character(CL)               :: auxflds       ! colon delimited string of field names
    integer                     :: n,n1,ncomp    ! field counter
    integer                     :: nfcnt         ! file counter
    integer                     :: nfile         ! file counter
    character(CS)               :: prefix        ! prefix for aux history file name
    logical                     :: found         ! temporary logical
    integer                     :: fieldcount
    character(CS), allocatable  :: fieldNameList(:)
    character(len=*), parameter :: subname=' (med_phases_history_init)'
    !---------------------------------------

    rc = ESMF_SUCCESS

    ! Get the internal state
    nullify(is_local%wrap)
    call ESMF_GridCompGetInternalState(gcomp, is_local, rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! Get model clock, start time, current time and time step
    call NUOPC_ModelGet(gcomp, modelClock=mclock,  rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockGet(mclock, startTime=mStartTime,  currTime=mCurrTime, timeStep=mtimestep, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_TimeIntervalGet(mtimestep, s=timestep_length, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if (mastertask) then
       write(logunit,*)
       write(logunit,'(a,2x,i8)') trim(subname)//" history clock timestep = ",timestep_length
    end if

    ! -----------------------------
    ! Instantaneous alarms
    ! -----------------------------

    ! Determine instantaneous mediator output frequency and type
    hist_option = 'none'
    hist_n = -999
    call NUOPC_CompAttributeGet(gcomp, name='history_option', isPresent=isPresent, isSet=isSet, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if (isPresent .and. isSet) then
       call NUOPC_CompAttributeGet(gcomp, name='history_option', value=hist_option, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       call NUOPC_CompAttributeGet(gcomp, name='history_n', value=cvalue, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       read(cvalue,*) hist_n
    end if

    ! Set alarms for instantaneous mediator history output
    ! Advance model clock to trigger alarms then reset model clock back to mcurrtime
    alarmname = 'alarm_history_inst_all'
    call med_time_alarmInit(mclock, alarm, option=hist_option, opt_n=hist_n, &
         reftime=mStartTime, alarmname=trim(alarmname), rc=rc)
    call ESMF_AlarmSet(alarm, clock=mclock, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockAdvance(mclock,rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockSet(mclock, currTime=mcurrtime, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if (mastertask) then
       write(logunit,'(a,2x,i8)') trim(subname)//" set instantaneous mediator history alarm "//&
            trim(alarmname)//"  with option "//trim(hist_option)//" and frequency ",hist_n
    end if
    do n = 1,ncomps
       if (is_local%wrap%comp_present(n)) then
          alarmname = 'alarm_history_inst_' // trim(compname(n))
          call med_time_alarmInit(mclock, alarm, option=hist_option, opt_n=hist_n, &
               reftime=mStartTime, alarmname=trim(alarmname), rc=rc)
          call ESMF_AlarmSet(alarm, clock=mclock, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call ESMF_ClockAdvance(mclock,rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call ESMF_ClockSet(mclock, currTime=mcurrtime, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          if (mastertask) then
             write(logunit,'(a,2x,i8)') trim(subname)//" set instantaneous mediator history alarm "//&
                  trim(alarmname)//"  with option "//trim(hist_option)//" and frequency ",hist_n
          end if
       end if
    end do

    ! -----------------------------
    ! Time average file initialization
    ! -----------------------------

    ! Determine time average mediator output frequency and type
    hist_option = 'none'
    hist_n = -999
    call NUOPC_CompAttributeGet(gcomp, name='history_avg_option', isPresent=isPresent, isSet=isSet, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if (isPresent .and. isSet) then
       call NUOPC_CompAttributeGet(gcomp, name='history_avg_option', value=hist_option, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       call NUOPC_CompAttributeGet(gcomp, name='history_avg_n', value=cvalue, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       read(cvalue,*) hist_n
    end if

    ! Set alarm for time averaged mediator history output
    alarmname = 'alarm_history_avg_all'
    call med_time_alarmInit(mclock, alarm, option=hist_option, opt_n=hist_n, &
         reftime=mStartTime, alarmname=trim(alarmname), rc=rc)
    call ESMF_AlarmSet(alarm, clock=mclock, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockAdvance(mclock,rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockSet(mclock, currTime=mcurrtime, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if (mastertask) then
       write(logunit,'(a,2x,i8)') trim(subname)//" set average mediator history alarm "//&
            trim(alarmname)//"  with option "//trim(hist_option)//" and frequency ",hist_n
    end if
    do n = 1,ncomps
       if (is_local%wrap%comp_present(n)) then
          alarmname = 'alarm_history_avg_' // trim(compname(n))
          call med_time_alarmInit(mclock, alarm, option=hist_option, opt_n=hist_n, &
               reftime=mStartTime, alarmname=trim(alarmname), rc=rc)
          call ESMF_AlarmSet(alarm, clock=mclock, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call ESMF_ClockAdvance(mclock,rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call ESMF_ClockSet(mclock, currTime=mcurrtime, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          if (mastertask) then
             write(logunit,'(a,2x,i8)') trim(subname)//" set average mediator history alarm "//&
                  trim(alarmname)//"  with option "//trim(hist_option)//" and frequency ",hist_n
          end if
       end if
    end do

    ! Create time average field bundles (module variables)
    if (hist_option /= 'never' .and. hist_option /= 'none') then
       call NUOPC_CompAttributeGet(gcomp, name='history_avg_n', value=cvalue, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       read(cvalue,*) hist_n
       do n = 1,ncomps
          ! accumulated import fields
          if (ESMF_FieldBundleIsCreated(is_local%wrap%FBimp(n,n),rc=rc)) then
             call FB_init(avgfiles_import(n)%FBaccum, is_local%wrap%flds_scalar_name, &
                  FBgeom=is_local%wrap%FBImp(n,n), STflds=is_local%wrap%NStateImp(n), rc=rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             call FB_reset(avgfiles_import(n)%FBaccum, czero, rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             avgfiles_import(n)%accumcnt = 0
          end if
          ! accumulated export fields
          if (ESMF_FieldBundleIsCreated(is_local%wrap%FBexp(n), rc=rc)) then
             call FB_init(avgfiles_export(n)%FBaccum, is_local%wrap%flds_scalar_name, &
                  FBgeom=is_local%wrap%FBExp(n), STflds=is_local%wrap%NstateExp(n), rc=rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             call FB_reset(avgfiles_export(n)%FBaccum, czero, rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             avgfiles_export(n)%accumcnt = 0
          end if
          ! accumulated atm/ocn flux on ocn mesh
          if (ESMF_FieldBundleIsCreated(is_local%wrap%FBMed_aoflux_o, rc=rc)) then
             call FB_init(avgfiles_aoflux_ocn%FBaccum, is_local%wrap%flds_scalar_name, &
                  FBgeom=is_local%wrap%FBMed_aoflux_o, FBflds=is_local%wrap%FBMed_aoflux_o, rc=rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             call FB_reset(avgfiles_aoflux_ocn%FBaccum, czero, rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             avgfiles_aoflux_ocn%accumcnt = 0
          end if
          ! accumulated atm/ocn flux on atm mesh
          if (ESMF_FieldBundleIsCreated(is_local%wrap%FBMed_aoflux_a, rc=rc)) then
             call FB_init(avgfiles_aoflux_atm%FBaccum, is_local%wrap%flds_scalar_name, &
                  FBgeom=is_local%wrap%FBMed_aoflux_a, FBflds=is_local%wrap%FBMed_aoflux_a, rc=rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             call FB_reset(avgfiles_aoflux_atm%FBaccum, czero, rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             avgfiles_aoflux_atm%accumcnt = 0
          end if
          ! accumulated ocean albedo on ocn mesh
          if (ESMF_FieldBundleIsCreated(is_local%wrap%FBMed_ocnalb_o, rc=rc)) then
             call FB_init(avgfiles_ocnalb_ocn%FBaccum, is_local%wrap%flds_scalar_name, &
                  FBgeom=is_local%wrap%FBMed_ocnalb_o, FBflds=is_local%wrap%FBMed_ocnalb_o, rc=rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             call FB_reset(avgfiles_ocnalb_ocn%FBaccum, czero, rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             avgfiles_ocnalb_ocn%accumcnt = 0
          end if
          ! accumulated ocean albedo on atm mesh
          if (ESMF_FieldBundleIsCreated(is_local%wrap%FBMed_ocnalb_a, rc=rc)) then
             call FB_init(avgfiles_ocnalb_atm%FBaccum, is_local%wrap%flds_scalar_name, &
                  FBgeom=is_local%wrap%FBMed_ocnalb_a, FBflds=is_local%wrap%FBMed_ocnalb_a, rc=rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             call FB_reset(avgfiles_ocnalb_atm%FBaccum, czero, rc)
             if (chkerr(rc,__LINE__,u_FILE_u)) return
             avgfiles_ocnalb_atm%accumcnt = 0
          end if
       end do
    end if

    ! -----------------------------
    ! Auxiliary file(s) initialization
    ! -----------------------------

    do ncomp = 2,ncomps ! skip the mediator

       ! Initialize number of aux files for this component to zero
       nfcnt = 0
       do nfile = 1,max_auxfiles
          ! Determine attribute prefix
          write(prefix,'(a,i0)') 'histaux_'//trim(compname(ncomp))//'2med_file',nfile

          ! Determine if on/off flag for this file exists
          call NUOPC_CompAttributeGet(gcomp, name=trim(prefix)//'_flag', isPresent=isPresent, isSet=isSet, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          if (isPresent .and. isSet) then
             call NUOPC_CompAttributeGet(gcomp, name=trim(prefix)//'_flag', value=cvalue, rc=rc)
             if (ChkErr(rc,__LINE__,u_FILE_u)) return
          end if
          ! If flag is on - then initailize auxfiles(ncomp,nfcnt)
          if (isPresent .and. isSet .and. (trim(cvalue) == 'on')) then
             ! Increment nfcnt
             nfcnt = nfcnt + 1

             ! Determine number of time samples per file
             call NUOPC_CompAttributeGet(gcomp, name=trim(prefix)//'_ntperfile', value=cvalue, rc=rc)
             if (ChkErr(rc,__LINE__,u_FILE_u)) return
             read(cvalue,*) auxfiles(ncomp,nfcnt)%ntperfile
             if (ChkErr(rc,__LINE__,u_FILE_u)) return

             ! Determine if will do time average
             call NUOPC_CompAttributeGet(gcomp, name=trim(prefix)//'_useavg', value=cvalue, rc=rc)
             if (ChkErr(rc,__LINE__,u_FILE_u)) return
             read(cvalue,*) auxfiles(ncomp,nfcnt)%useavg

             ! Determine fields that will be output to auxhist files
             ! First dtermine the colon delimited field names for this file
             call NUOPC_CompAttributeGet(gcomp, name=trim(prefix)//'_flds', value=auxflds, rc=rc)
             if (ChkErr(rc,__LINE__,u_FILE_u)) return
             if (auxflds == 'all') then
                ! Output all fields sent to the mediator from ncomp to the auxhist files
                call ESMF_FieldBundleGet(is_local%wrap%FBImp(ncomp,ncomp), &
                     fieldCount=fieldCount, rc=rc)
                if (chkerr(rc,__LINE__,u_FILE_u)) return
                allocate(auxfiles(ncomp,nfcnt)%flds(fieldcount))
                call ESMF_FieldBundleGet(is_local%wrap%FBImp(ncomp,ncomp), &
                     fieldNameList=auxfiles(ncomp,nfcnt)%flds, rc=rc)
                if (chkerr(rc,__LINE__,u_FILE_u)) return
             else
                ! Translate the colon deliminted string (auxflds) into a character array (fieldnamelist)
                ! Note that the following call allocates the memory for fieldnamelist
                call med_phases_history_get_auxflds(auxflds, fieldnamelist, rc)

                ! Remove all fields from fieldnamelist that are not in FBImp(ncomp,ncomp)
                fieldCount = size(fieldnamelist)
                do n = 1,fieldcount
                   if (.not. FB_fldchk(is_local%wrap%FBImp(ncomp,ncomp), &
                        trim(fieldnamelist(n)), rc)) then
                      do n1 = n, fieldCount-1
                         fieldnamelist(n1) = fieldnamelist(n1+1)
                      end do
                      fieldCount = fieldCount - 1
                   end if
                end do

                ! Create auxfiles(ncomp,nfcnt)%flds array
                allocate(auxfiles(ncomp,nfcnt)%flds(fieldcount))
                do n = 1,fieldcount
                   auxfiles(ncomp,nfcnt)%flds(n) = trim(fieldnamelist(n))
                end do

                ! Deallocate memory from fieldnamelist
                deallocate(fieldnamelist) ! this was allocated in med_phases_history_get_auxflds
             end if

             ! Create FBaccum if averaging is on
             if (auxfiles(ncomp,nfcnt)%useavg) then
                ! First duplicate all fields in FBImp(ncomp,ncomp)
                call ESMF_LogWrite(trim(subname)// ": calling FB_init for FBaccum(ncomp)", ESMF_LOGMSG_INFO)
                call FB_init(auxfiles(ncomp,nfcnt)%FBaccum, is_local%wrap%flds_scalar_name, &
                     FBgeom=is_local%wrap%FBImp(ncomp,ncomp), &
                     STflds=is_local%wrap%NStateImp(ncomp), rc=rc)
                if (chkerr(rc,__LINE__,u_FILE_u)) return

                ! Now remove all fields from FBAccum that are not in the input flds list
                call ESMF_FieldBundleGet(is_local%wrap%FBImp(ncomp,ncomp), &
                     fieldCount=fieldCount, rc=rc)
                if (chkerr(rc,__LINE__,u_FILE_u)) return
                allocate(fieldNameList(fieldCount))
                call ESMF_FieldBundleGet(is_local%wrap%FBImp(ncomp,ncomp), &
                     fieldNameList=fieldNameList, rc=rc)
                if (chkerr(rc,__LINE__,u_FILE_u)) return
                do n = 1,size(fieldnamelist)
                   found = .false.
                   do n1 = 1,size(auxfiles(ncomp,nfcnt)%flds)
                      if (trim(fieldnamelist(n)) == trim(auxfiles(ncomp,nfcnt)%flds(n1))) then
                         found = .true.
                         exit
                      end if
                   end do
                   if (.not. found) then
                      call ESMF_FieldBundleRemove(auxfiles(ncomp,nfcnt)%FBaccum, &
                           fieldnamelist(n:n), rc=rc)
                      if (chkerr(rc,__LINE__,u_FILE_u)) return
                   end if
                end do
                deallocate(fieldnameList)
             end if

             ! Determine history alarm for this file - advance nextAlarm so it won't ring on the first timestep
             call NUOPC_CompAttributeGet(gcomp, name=trim(prefix)//'_deltat', value=cvalue, rc=rc)
             if (ChkErr(rc,__LINE__,u_FILE_u)) return
             if (cvalue == 'every_nstep') then
                ! write out output each coupling interval
                call ESMF_ClockGet(mclock, timeStep=alarmInterval, rc=rc)
                if (ChkErr(rc,__LINE__,u_FILE_u)) return
                call ESMF_TimeIntervalGet(alarmInterval, s=auxfiles(ncomp,nfcnt)%deltat, rc=rc)
                if (ChkErr(rc,__LINE__,u_FILE_u)) return
             else
                ! get the deltat from the attribute
                read(cvalue,*) auxfiles(ncomp,nfcnt)%deltat
                call ESMF_TimeIntervalSet(AlarmInterval, s=auxfiles(ncomp,nfcnt)%deltat, rc=rc)
                if (ChkErr(rc,__LINE__,u_FILE_u)) return
             end if
             call NUOPC_CompAttributeGet(gcomp, name=trim(prefix)//'_auxname', &
                  value=auxfiles(ncomp,nfcnt)%auxname, rc=rc)
             if (ChkErr(rc,__LINE__,u_FILE_u)) return
             write(auxfiles(ncomp,nfcnt)%alarmname,'(a,i0)') 'alarm_auxhist_'//&
                  trim(auxfiles(ncomp,nfcnt)%auxname)//'_', auxfiles(ncomp,nfcnt)%deltat
             if (mastertask) then
                write(logunit,'(a)') trim(subname) //' creating auxiliary history alarm '//&
                     trim(auxfiles(ncomp,nfcnt)%alarmname)
             end if
             nextAlarm = mstarttime - AlarmInterval
             do while (nextAlarm <= mcurrtime)
                nextAlarm = nextAlarm + AlarmInterval
             enddo
             alarm = ESMF_AlarmCreate( name=auxfiles(ncomp,nfcnt)%alarmname, clock=mclock, &
                  ringTime=nextAlarm, ringInterval=alarmInterval, rc=rc)
             if (ChkErr(rc,__LINE__,u_FILE_u)) return
          end if ! end of isPresent and isSet and  if flag is on
       end do ! end of loop over files (nfiles)

       ! Set number of aux files for this component
       num_auxfiles(ncomp) = nfcnt
    end do ! end of loop over components (ncomp)

    ! Get file name variables
    call NUOPC_CompAttributeGet(gcomp, name='case_name', value=case_name, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call NUOPC_CompAttributeGet(gcomp, name='inst_suffix', isPresent=isPresent, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if(isPresent) then
       call NUOPC_CompAttributeGet(gcomp, name='inst_suffix', value=inst_tag, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    else
       inst_tag = ""
    endif

    if (mastertask) write(logunit,*)

  end subroutine med_phases_history_init

  !===============================================================================
  subroutine med_phases_history_write(gcomp, rc)
    ! --------------------------------------
    ! Write mediator history file for all variables
    ! This is a phase called by the run sequence
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    character(len=*), parameter :: subname='(med_phases_history_write)'
    !---------------------------------------
    rc = ESMF_SUCCESS

    call t_startf('MED:'//subname)
    call med_phases_history_write_hfile(gcomp, 'all', 'alarm_history_inst_all', .false., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call med_phases_history_write_hfile(gcomp, 'all', 'alarm_history_avg_all', .true., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call t_stopf('MED:'//subname)

  end subroutine med_phases_history_write

  !===============================================================================
  subroutine med_phases_history_write_med(gcomp, rc)
    ! --------------------------------------
    ! Write mediator history file for med variables - only instantaneous files are written
    ! This writes out ocean albedoes and atm/ocean fluxes computed by the mediator
    ! along with the fractions computed by the mediator
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    character(len=*), parameter :: subname='(med_phases_history_write_med)'
    !---------------------------------------
    rc = ESMF_SUCCESS

    call t_startf('MED:'//subname)
    call med_phases_history_write_hfile(gcomp, 'med', 'alarm_history_inst_med', .false., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call t_stopf('MED:'//subname)

  end subroutine med_phases_history_write_med

  !===============================================================================
  subroutine med_phases_history_write_atm(gcomp, rc)
    ! --------------------------------------
    ! Write mediator history file for atm variables
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    integer :: n
    character(len=*), parameter :: subname='(med_phases_history_write_atm)'
    !---------------------------------------
    rc = ESMF_SUCCESS
    call t_startf('MED:'//subname)
    call med_phases_history_write_hfile(gcomp, 'atm', 'alarm_history_inst_atm', .false., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call med_phases_history_write_hfile(gcomp, 'atm', 'alarm_history_avg_atm', .true., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    do n = 1,num_auxfiles(compatm)
       call med_phases_history_write_hfileaux(gcomp, case_name, inst_tag, n, compatm, auxfiles(compatm,n), rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    end do
    call t_stopf('MED:'//subname)
  end subroutine med_phases_history_write_atm

  !===============================================================================
  subroutine med_phases_history_write_ice(gcomp, rc)
    ! --------------------------------------
    ! Write mediator history file for ice variables
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    integer :: n
    character(len=*), parameter :: subname='(med_phases_history_write_ice)'
    !---------------------------------------
    rc = ESMF_SUCCESS
    call t_startf('MED:'//subname)
    call med_phases_history_write_hfile(gcomp, 'ice', 'alarm_history_inst_ice', .false., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call med_phases_history_write_hfile(gcomp, 'ice', 'alarm_history_avg_ice', .true., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    do n = 1,num_auxfiles(compice)
       call med_phases_history_write_hfileaux(gcomp, case_name, inst_tag, n, compice, auxfiles(compice,n), rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    end do
    call t_stopf('MED:'//subname)

  end subroutine med_phases_history_write_ice

  !===============================================================================
  subroutine med_phases_history_write_glc(gcomp, rc)
    ! --------------------------------------
    ! Write mediator history file for glc variables
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    integer :: n
    character(len=*), parameter :: subname='(med_phases_history_write_glc)'
    !---------------------------------------
    rc = ESMF_SUCCESS
    call t_startf('MED:'//subname)
    call med_phases_history_write_hfile(gcomp, 'glc', 'alarm_history_inst_glc', .false., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call med_phases_history_write_hfile(gcomp, 'glc', 'alarm_history_avg_glc', .true., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    do n = 1,num_auxfiles(compglc)
       call med_phases_history_write_hfileaux(gcomp, case_name, inst_tag, n, compglc, auxfiles(compglc,n), rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    end do
    call t_stopf('MED:'//subname)
  end subroutine med_phases_history_write_glc

  !===============================================================================
  subroutine med_phases_history_write_lnd(gcomp, rc)
    ! --------------------------------------
    ! Write mediator history file for lnd variables
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    integer :: n
    character(len=*), parameter :: subname='(med_phases_history_write_lnd)'
    !---------------------------------------
    rc = ESMF_SUCCESS
    call t_startf('MED:'//subname)
    call med_phases_history_write_hfile(gcomp, 'lnd', 'alarm_history_inst_lnd', .false., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call med_phases_history_write_hfile(gcomp, 'lnd', 'alarm_history_avg_lnd', .true., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    do n = 1,num_auxfiles(complnd)
       call med_phases_history_write_hfileaux(gcomp, case_name, inst_tag, n, complnd, auxfiles(complnd,n), rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    end do
    call t_stopf('MED:'//subname)
  end subroutine med_phases_history_write_lnd

  !===============================================================================
  subroutine med_phases_history_write_ocn(gcomp, rc)
    ! --------------------------------------
    ! Write mediator history file for ocn variables
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    integer :: n
    character(len=*), parameter :: subname='(med_phases_history_write_ocn)'
    !---------------------------------------
    rc = ESMF_SUCCESS
    call t_startf('MED:'//subname)
    call med_phases_history_write_hfile(gcomp, 'ocn', 'alarm_history_inst_ocn', .false., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call med_phases_history_write_hfile(gcomp, 'ocn', 'alarm_history_avg_ocn', .true., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    do n = 1,num_auxfiles(compocn)
       call med_phases_history_write_hfileaux(gcomp, case_name, inst_tag, n, compocn, auxfiles(compocn,n), rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    end do
    call t_stopf('MED:'//subname)
  end subroutine med_phases_history_write_ocn

  !===============================================================================
  subroutine med_phases_history_write_rof(gcomp, rc)
    ! --------------------------------------
    ! Write mediator history file for rof variables
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    integer :: n
    character(len=*), parameter :: subname='(med_phases_history_write_rof)'
    !---------------------------------------
    rc = ESMF_SUCCESS
    call t_startf('MED:'//subname)
    call med_phases_history_write_hfile(gcomp, 'rof', 'alarm_history_inst_rof', .false., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call med_phases_history_write_hfile(gcomp, 'rof', 'alarm_history_avg_rof', .true., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    do n = 1,num_auxfiles(comprof)
       call med_phases_history_write_hfileaux(gcomp, case_name, inst_tag, n, comprof, auxfiles(comprof,n), rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    end do
    call t_stopf('MED:'//subname)
  end subroutine med_phases_history_write_rof

  !===============================================================================
  subroutine med_phases_history_write_wav(gcomp, rc)
    ! --------------------------------------
    ! Write mediator history file for wav variables
    ! --------------------------------------

    ! input/output variables
    type(ESMF_GridComp)  :: gcomp
    integer, intent(out) :: rc

    ! local variables
    integer :: n
    character(len=*), parameter :: subname='(med_phases_history_write_wav)'
    !---------------------------------------
    rc = ESMF_SUCCESS
    call t_startf('MED:'//subname)
    call med_phases_history_write_hfile(gcomp, 'wav', 'alarm_history_inst_wav', .false., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call med_phases_history_write_hfile(gcomp, 'wav', 'alarm_history_avg_wav', .true., rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    do n = 1,num_auxfiles(compwav)
       call med_phases_history_write_hfileaux(gcomp, case_name, inst_tag, n, compwav, auxfiles(compwav,n), rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    end do
    call t_stopf('MED:'//subname)
  end subroutine med_phases_history_write_wav

  !===============================================================================
  subroutine med_phases_history_write_hfile(gcomp, comptype, alarmname, doavg, rc)

    ! input/output variables
    type(ESMF_GridComp) , intent(inout) :: gcomp
    character(len=*)    , intent(in)    :: comptype
    character(len=*)    , intent(in)    :: alarmname
    logical             , intent(in)    :: doavg
    integer             , intent(out)   :: rc

    ! local variables
    type(InternalState)     :: is_local
    type(ESMF_VM)           :: vm
    type(ESMF_Clock)        :: mclock
    type(ESMF_Alarm)        :: alarm
    type(ESMF_Time)         :: starttime
    type(ESMF_Time)         :: currtime
    type(ESMF_Time)         :: nexttime
    type(ESMF_Calendar)     :: calendar          ! calendar type
    type(ESMF_TimeInterval) :: timediff(2)       ! time bounds upper and lower relative to start
    type(ESMF_TimeInterval) :: ringInterval      ! alarm interval
    real(r8)                :: tbnds(2)          ! CF1.0 time bounds
    integer                 :: i,j,m,n
    integer                 :: nx,ny             ! global grid size
    character(CL)           :: time_units        ! units of time variable
    character(CL)           :: hist_file
    real(r8)                :: days_since        ! Time interval since reference time
    real(r8)                :: avg_time          ! Time coordinate output
    logical                 :: whead,wdata       ! for writing restart/history cdf files
    integer                 :: iam
    logical                 :: write_now
    integer                 :: yr,mon,day,sec    ! time units
    character(len=*), parameter :: subname='(med_phases_history_write_hfile)'
    !---------------------------------------

    rc = ESMF_SUCCESS

    ! Get the communicator and localpet
    call ESMF_GridCompGet(gcomp, vm=vm, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_VMGet(vm, localPet=iam, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! Get the internal state
    nullify(is_local%wrap)
    call ESMF_GridCompGetInternalState(gcomp, is_local, rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! Get the history file alarm and determine if alarm is ringing
    call NUOPC_ModelGet(gcomp, modelClock=mclock,  rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockGetAlarm(mclock, alarmname=trim(alarmname), alarm=alarm, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if (ESMF_AlarmIsRinging(alarm, rc=rc)) then
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       if (debug_alarms) then
         call med_phases_history_output_alarminfo(mclock, alarm, alarmname, rc)
         if (ChkErr(rc,__LINE__,u_FILE_u)) return
      end if
       ! Set write_now flag
       write_now = .true.
       ! Turn ringer off
       call ESMF_AlarmRingerOff(alarm, rc=rc )
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    else
       write_now = .false.
    end if

    ! Accumulate if alarm is not on - other wise average
    if (doavg) then
       do n = 1,ncomps
          if (comptype == 'all' .or. comptype == trim(compname(n))) then
             if (write_now) then
                if (ESMF_FieldBundleIsCreated(avgfiles_import(n)%FBaccum)) then
                   call FB_average(avgfiles_import(n)%FBaccum, avgfiles_import(n)%accumcnt, rc=rc)
                   if (ChkErr(rc,__LINE__,u_FILE_u)) return
                   avgfiles_import(n)%accumcnt = 0
                end if
                if (ESMF_FieldBundleIsCreated(avgfiles_export(n)%FBaccum)) then
                   call FB_average(avgfiles_export(n)%FBaccum, avgfiles_export(n)%accumcnt, rc=rc)
                   if (ChkErr(rc,__LINE__,u_FILE_u)) return
                   avgfiles_export(n)%accumcnt = 0
                end if
             else
                if (ESMF_FieldBundleIsCreated(avgfiles_import(n)%FBaccum)) then
                   call FB_accum(avgfiles_import(n)%FBaccum, is_local%wrap%FBImp(n,n), rc=rc)
                   if (ChkErr(rc,__LINE__,u_FILE_u)) return
                   avgfiles_import(n)%accumcnt = avgfiles_import(n)%accumcnt + 1
                end if
                if (ESMF_FieldBundleIsCreated(avgfiles_export(n)%FBaccum)) then
                   call FB_accum(avgfiles_export(n)%FBaccum, is_local%wrap%FBExp(n), rc=rc)
                   if (ChkErr(rc,__LINE__,u_FILE_u)) return
                   avgfiles_export(n)%accumcnt = avgfiles_export(n)%accumcnt + 1
                end if
             end if
          end if
       end do
    end if

    ! Check if history alarm is ringing - and if so write the mediator history file
    if (write_now) then

       ! Determine history file name and time units
       call med_phases_history_get_filename(gcomp, doavg, comptype, hist_file, time_units, days_since, rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

       ! Set tbnds and avg_time if doing averaging
       if (doavg) then
          call ESMF_ClockGet(mclock, currtime=currtime, starttime=starttime, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call ESMF_ClockGetNextTime(mclock, nextTime=nexttime, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call ESMF_AlarmGet(alarm, ringInterval=ringInterval, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          timediff(2) = nexttime - starttime
          timediff(1) = nexttime - ringinterval - starttime
          call ESMF_TimeIntervalGet(timediff(2), d_r8=tbnds(2), rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call ESMF_TimeIntervalGet(timediff(1), d_r8=tbnds(1), rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          avg_time = 0.5_r8 * (tbnds(1) + tbnds(2))
       end if

       ! Create history file
       call med_io_wopen(hist_file, vm, iam, clobber=.true.)
       do m = 1,2
          if (m == 1) then
             whead = .true.
             wdata = .false.
          else if (m == 2) then
             whead = .false.
             wdata = .true.
             call med_io_enddef(hist_file)
          end if

          ! Write time values (tbnds does not appear in instantaneous output)
          call ESMF_ClockGet(mclock, calendar=calendar, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          if (doavg) then
             call med_io_write(hist_file, iam, time_units=time_units, calendar=calendar, time_val=avg_time, &
                  nt=1, tbnds=tbnds, whead=whead, wdata=wdata, rc=rc)
             if (ChkErr(rc,__LINE__,u_FILE_u)) return
          else
             call med_io_write(hist_file, iam, time_units=time_units, calendar=calendar, time_val=days_since, &
                  nt=1, whead=whead, wdata=wdata, rc=rc)
             if (ChkErr(rc,__LINE__,u_FILE_u)) return
          end if

          ! Write import and export field bundles
          do n = 2,ncomps ! skip the mediator here
             if (comptype == 'all' .or. comptype == trim(compname(n))) then
                if (is_local%wrap%comp_present(n)) then
                   nx = is_local%wrap%nx(n)
                   ny = is_local%wrap%ny(n)
                   if (ESMF_FieldBundleIsCreated(is_local%wrap%FBimp(n,n),rc=rc)) then
                      if (doavg) then
                         call med_io_write(hist_file, iam, avgfiles_import(n)%FBaccum, &
                              nx=nx, ny=ny, nt=1, whead=whead, wdata=wdata, &
                              pre=trim(compname(n))//'Imp', rc=rc)
                         if (ChkErr(rc,__LINE__,u_FILE_u)) return
                      else
                         call med_io_write(hist_file, iam, is_local%wrap%FBimp(n,n), &
                              nx=nx, ny=ny, nt=1, whead=whead, wdata=wdata, &
                              pre=trim(compname(n))//'Imp', rc=rc)
                         if (ChkErr(rc,__LINE__,u_FILE_u)) return
                      end if
                   endif
                   if (ESMF_FieldBundleIsCreated(is_local%wrap%FBexp(n),rc=rc)) then
                      if (doavg) then
                         call med_io_write(hist_file, iam, avgfiles_export(n)%FBaccum, &
                              nx=nx, ny=ny, nt=1, whead=whead, wdata=wdata, &
                              pre=trim(compname(n))//'Exp', rc=rc)
                         if (ChkErr(rc,__LINE__,u_FILE_u)) return
                      else
                         call med_io_write(hist_file, iam, is_local%wrap%FBexp(n), &
                              nx=nx, ny=ny, nt=1, whead=whead, wdata=wdata, &
                              pre=trim(compname(n))//'Exp', rc=rc)
                         if (ChkErr(rc,__LINE__,u_FILE_u)) return
                      end if
                   endif
                endif
             end if
          enddo

          ! Write mediator fractions
          ! Also write atm/ocn fluxes and ocean albedoes if field bundles are created
          if (.not. doavg) then
             if (comptype == 'all' .or. comptype == 'med') then
                do n = 2,ncomps ! skip the mediator here
                   nx = is_local%wrap%nx(n)
                   ny = is_local%wrap%ny(n)
                   if (ESMF_FieldBundleIsCreated(is_local%wrap%FBFrac(n),rc=rc)) then
                      call med_io_write(hist_file, iam, is_local%wrap%FBFrac(n), &
                           nx=nx, ny=ny, nt=1, whead=whead, wdata=wdata, &
                           pre='Med_frac_'//trim(compname(n)), rc=rc)
                      if (ChkErr(rc,__LINE__,u_FILE_u)) return
                   end if
                end do
                if (ESMF_FieldBundleIsCreated(is_local%wrap%FBMed_ocnalb_o,rc=rc)) then
                   nx = is_local%wrap%nx(compocn)
                   ny = is_local%wrap%ny(compocn)
                   call med_io_write(hist_file, iam, is_local%wrap%FBMed_ocnalb_o, &
                        nx=nx, ny=ny, nt=1, whead=whead, wdata=wdata, pre='Med_alb_ocn', rc=rc)
                end if
                if (ESMF_FieldBundleIsCreated(is_local%wrap%FBMed_aoflux_o,rc=rc)) then
                   nx = is_local%wrap%nx(compocn)
                   ny = is_local%wrap%ny(compocn)
                   call med_io_write(hist_file, iam, is_local%wrap%FBMed_aoflux_o, &
                        nx=nx, ny=ny, nt=1, whead=whead, wdata=wdata, pre='Med_aoflux_ocn', rc=rc)
                end if
                if (ESMF_FieldBundleIsCreated(is_local%wrap%FBMed_ocnalb_a,rc=rc)) then
                   nx = is_local%wrap%nx(compatm)
                   ny = is_local%wrap%ny(compatm)
                   call med_io_write(hist_file, iam, is_local%wrap%FBMed_ocnalb_a, &
                        nx=nx, ny=ny, nt=1, whead=whead, wdata=wdata, pre='Med_alb_atm', rc=rc)
                end if
                if (ESMF_FieldBundleIsCreated(is_local%wrap%FBMed_aoflux_a,rc=rc)) then
                   nx = is_local%wrap%nx(compatm)
                   ny = is_local%wrap%ny(compatm)
                   call med_io_write(hist_file, iam, is_local%wrap%FBMed_aoflux_a, &
                        nx=nx, ny=ny, nt=1, whead=whead, wdata=wdata, pre='Med_aoflux_atm', rc=rc)
                end if
             end if
          end if

       end do ! end of loop over m

       ! Close file
       call med_io_close(hist_file, iam, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

    end if ! end of if-alarm is ringingblock

  end subroutine med_phases_history_write_hfile

  !===============================================================================
  subroutine med_phases_history_write_hfileaux(gcomp, case_name, inst_tag, &
       nfile_index, comp_index, auxfile, rc)

    ! input/output variables
    type(ESMF_GridComp) , intent(inout) :: gcomp
    character(len=*)    , intent(in)    :: case_name
    character(len=*)    , intent(in)    :: inst_tag
    integer             , intent(in)    :: nfile_index
    integer             , intent(in)    :: comp_index
    type(auxfile_type)  , intent(inout) :: auxfile
    integer             , intent(out)   :: rc

    ! local variables
    type(InternalState)     :: is_local
    type(ESMF_VM)           :: vm
    type(ESMF_Clock)        :: mclock
    type(ESMF_Alarm)        :: alarm
    type(ESMF_Time)         :: starttime
    type(ESMF_Time)         :: currtime
    type(ESMF_Time)         :: nexttime
    type(ESMF_Calendar)     :: calendar          ! calendar type
    type(ESMF_TimeInterval) :: timediff(2)       ! time bounds upper and lower relative to start
    type(ESMF_TimeInterval) :: ringInterval      ! alarm interval
    character(CS)           :: timestr           ! yr-mon-day-sec string
    character(CL)           :: time_units        ! units of time variable
    real(r8)                :: avg_time          ! Time coordinate output
    integer                 :: nx,ny             ! global grid size
    logical                 :: whead,wdata       ! for writing restart/history cdf files
    logical                 :: write_now         ! if true, write time sample to file
    integer                 :: iam               ! mpi task
    integer                 :: start_ymd         ! Starting date YYYYMMDD
    integer                 :: yr,mon,day,sec    ! time units
    real(r8)                :: tbnds(2)      ! CF1.0 time bounds
    character(len=*), parameter :: subname='(med_phases_history_write_hfileaux)'
    !---------------------------------------

    rc = ESMF_SUCCESS

    ! Get the communicator and localpet
    call ESMF_GridCompGet(gcomp, vm=vm, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_VMGet(vm, localPet=iam, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! Get the internal state
    nullify(is_local%wrap)
    call ESMF_GridCompGetInternalState(gcomp, is_local, rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! Determine time info
    ! Use nexttime rather than currtime for the time difference form
    ! start since that is the time at the end of the time step
    call NUOPC_ModelGet(gcomp, modelClock=mclock,  rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockGet(mclock, currtime=currtime, starttime=starttime, calendar=calendar, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockGetNextTime(mclock, nextTime=nexttime, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockGetAlarm(mclock, alarmname=trim(auxfile%alarmname), alarm=alarm, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_AlarmGet(alarm, ringInterval=ringInterval, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    timediff(2) = nexttime - starttime
    timediff(1) = nexttime - ringinterval - starttime
    call ESMF_TimeIntervalGet(timediff(2), d_r8=tbnds(2), rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_TimeIntervalGet(timediff(1), d_r8=tbnds(1), rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    avg_time = 0.5_r8 * (tbnds(1) + tbnds(2))

    write_now = .false.
    if (ESMF_AlarmIsRinging(alarm, rc=rc)) then
       write_now = .true.
       call ESMF_AlarmRingerOff( alarm, rc=rc )
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
       if (debug_alarms) then
          call med_phases_history_output_alarminfo(mclock, alarm, auxfile%alarmname, rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
       end if
    end if

    ! Do accumulation if needed
    if (auxfile%useavg) then
       if (write_now) then
          call FB_average(auxfile%FBaccum, auxfile%accumcnt, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          auxfile%accumcnt = 0
       else
          call FB_accum(auxfile%FBaccum, is_local%wrap%FBImp(comp_index,comp_index), rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          auxfile%accumcnt = auxfile%accumcnt + 1
       endif
    end if

    ! Write time sample to file
    if ( write_now ) then

       ! Increment number of time samples on file
       auxfile%nt = auxfile%nt + 1

       ! Set shorthand variables
       nx = is_local%wrap%nx(comp_index)
       ny = is_local%wrap%ny(comp_index)

       ! Write  header
       if (auxfile%nt == 1) then

          ! determine history file name
          call ESMF_ClockGet(mclock, currtime=currtime, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call ESMF_TimeGet(currtime,yy=yr, mm=mon, dd=day, s=sec, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          write(timestr,'(i4.4,a,i2.2,a,i2.2,a,i5.5)') yr,'-',mon,'-',day,'-',sec
          write(auxfile%histfile, "(8a)") &
               trim(case_name),'.cpl',trim(inst_tag),'.hx.', trim(auxfile%auxname),'.',trim(timestr), '.nc'

          ! open file
          call med_io_wopen(auxfile%histfile, vm, iam, file_ind=nfile_index, clobber=.true.)

          ! define time units
          call ESMF_TimeGet(starttime, yy=yr, mm=mon, dd=day, s=sec, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call med_io_ymd2date(yr,mon,day,start_ymd)
          time_units = 'days since ' // trim(med_io_date2yyyymmdd(start_ymd)) // ' ' // med_io_sec2hms(sec, rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return

          ! define time variables
          call med_io_write(auxfile%histfile, iam, time_units, calendar, avg_time, &
               nt=auxfile%nt, tbnds=tbnds, whead=.true., wdata=.false., file_ind=nfile_index, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return

          ! define data variables with a time dimension (include the nt argument below)
          call med_io_write(auxfile%histfile, iam, is_local%wrap%FBimp(comp_index,comp_index), &
               nx=nx, ny=ny, nt=auxfile%nt, whead=.true., wdata=.false., pre=trim(compname(comp_index))//'Imp', &
               flds=auxfile%flds, file_ind=nfile_index, use_float=.true., rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return

          ! end definition phase
          call med_io_enddef(auxfile%histfile, file_ind=nfile_index)

       end if

       ! Write time variables for time nt
       call med_io_write(auxfile%histfile, iam, time_units, calendar, avg_time, &
            nt=auxfile%nt, tbnds=tbnds, whead=.false., wdata=.true., file_ind=nfile_index, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return

       ! Write data variables for time nt
       if (auxfile%useavg) then
          call med_io_write(auxfile%histfile, iam, auxfile%FBaccum, &
               nx=nx, ny=ny, nt=auxfile%nt, whead=.false., wdata=.true., pre=trim(compname(comp_index))//'Imp', &
               flds=auxfile%flds, file_ind=nfile_index, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          call FB_reset(auxfile%FBaccum, value=czero, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
       else
          call med_io_write(auxfile%histfile, iam, is_local%wrap%FBimp(comp_index,comp_index), &
               nx=nx, ny=ny, nt=auxfile%nt, whead=.false., wdata=.true., pre=trim(compname(comp_index))//'Imp', &
               flds=auxfile%flds, file_ind=nfile_index, rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
       end if

       ! Close file
       if (auxfile%nt == auxfile%ntperfile) then
          call med_io_close(auxfile%histfile, iam, file_ind=nfile_index,  rc=rc)
          if (ChkErr(rc,__LINE__,u_FILE_u)) return
          auxfile%nt = 0
       end if

    end if ! end of write_now if-block

  end subroutine med_phases_history_write_hfileaux

  !===============================================================================
  subroutine med_phases_history_get_filename(gcomp, doavg, comptype, hist_file, time_units, days_since, rc)

    ! input/output variables
    type(ESMF_GridComp) , intent(inout) :: gcomp
    logical             , intent(in)    :: doavg
    character(len=*)    , intent(in)    :: comptype
    character(len=*)    , intent(out)   :: hist_file
    character(len=*)    , intent(out)   :: time_units
    real(r8)            , intent(out)   :: days_since ! Time interval since reference time
    integer             , intent(out)   :: rc

    ! local variables
    type(ESMF_Clock)        :: mclock
    type(ESMF_Time)         :: currtime
    type(ESMF_Time)         :: starttime
    type(ESMF_Time)         :: nexttime
    type(ESMF_TimeInterval) :: timediff       ! Used to calculate curr_time
    type(ESMF_Calendar)     :: calendar       ! calendar type
    character(len=CS)       :: currtimestr
    character(len=CS)       :: nexttimestr
    integer                 :: start_ymd      ! Starting date YYYYMMDD
    integer                 :: yr,mon,day,sec ! time units
    logical                 :: isPresent
    character(CL)           :: case_name      ! case name
    character(CS)           :: inst_tag   ! instance tag
    character(len=CS)        :: histstr
    character(len=*), parameter :: subname='(med_phases_history_get_timeunits)'
    !---------------------------------------

    rc = ESMF_SUCCESS

    ! Get case_name and inst_tag
    call NUOPC_CompAttributeGet(gcomp, name='case_name', value=case_name, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call NUOPC_CompAttributeGet(gcomp, name='inst_suffix', isPresent=isPresent, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if(isPresent) then
       call NUOPC_CompAttributeGet(gcomp, name='inst_suffix', value=inst_tag, rc=rc)
       if (ChkErr(rc,__LINE__,u_FILE_u)) return
    else
       inst_tag = ""
    endif

    ! Get time unit attribute value for variables
    call NUOPC_ModelGet(gcomp, modelClock=mclock, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockGet(mclock, currtime=currtime, starttime=starttime, calendar=calendar, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockGetNextTime(mclock, nextTime=nexttime, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_TimeGet(currtime,yy=yr, mm=mon, dd=day, s=sec, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    write(currtimestr,'(i4.4,a,i2.2,a,i2.2,a,i5.5)') yr,'-',mon,'-',day,'-',sec
    call ESMF_TimeGet(nexttime,yy=yr, mm=mon, dd=day, s=sec, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    write(nexttimestr,'(i4.4,a,i2.2,a,i2.2,a,i5.5)') yr,'-',mon,'-',day,'-',sec
    timediff = nexttime - starttime
    call ESMF_TimeIntervalGet(timediff, d=day, s=sec, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    days_since = day + sec/real(SecPerDay,R8)
    call ESMF_TimeGet(starttime, yy=yr, mm=mon, dd=day, s=sec, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call med_io_ymd2date(yr,mon,day,start_ymd)
    time_units = 'days since ' // trim(med_io_date2yyyymmdd(start_ymd)) // ' ' // med_io_sec2hms(sec, rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    ! Determine history file name
    ! Use nexttimestr rather than currtimestr here since that is the time at the end of
    ! the timestep and is preferred for history file names
    if (doavg) then
       histstr = 'ha.'
    else
       histstr = 'hi.'
    end if
    if (trim(comptype) /= 'all') then
       histstr = trim(histstr) // trim(comptype) // '.'
    end if
    write(hist_file,"(6a)") trim(case_name),'.cpl.',trim(inst_tag),trim(histstr),trim(nexttimestr),'.nc'
    if (mastertask) then
       write(logunit,*)
       write(logunit,' (a)') trim(subname)//": writing mediator history file "//trim(hist_file)
       write(logunit,' (a)') trim(subname)//": currtime = "//trim(currtimestr)
       write(logunit,' (a)') trim(subname)//": nexttime = "//trim(nexttimestr)
    end if

  end subroutine med_phases_history_get_filename

  !===============================================================================
  subroutine med_phases_history_get_auxflds(str, flds, rc)

    ! input/output variables
    character(len=*)               , intent(in)  :: str     ! colon deliminted string to search
    character(len=*) , allocatable , intent(out) :: flds(:) ! memory will be allocate for flds
    integer                        , intent(out) :: rc

    ! local variables
    integer          :: i,k,n ! generic indecies
    integer          :: nflds ! allocatable size of flds
    integer          :: count ! counts occurances of char
    integer          :: kFlds ! number of fields in list
    integer          :: i0,i1 ! name = list(i0:i1)
    integer          :: nChar ! temporary
    logical          :: valid ! check if str is valid
    !---------------------------------------

    rc = ESMF_SUCCESS

    ! check that this is a str is a valid colon dlimited list
    valid = .true.
    nChar = len_trim(str)
    if (nChar < 1) then                     ! list is an empty string
       valid = .false.
    else if (str(1:1) == ':') then          ! first char is delimiter
       valid = .false.
    else if (str(nChar:nChar) == ':') then  ! last  char is delimiter
       valid = .false.
    else if (index(trim(str)," ") > 0) then ! white-space in a field name
       valid = .false.
    end if
    if (.not. valid) then
       if (mastertask) write(logunit,*) "ERROR: invalid list = ",trim(str)
       call ESMF_LogWrite("ERROR: invalid list = "//trim(str), ESMF_LOGMSG_ERROR)
       rc = ESMF_FAILURE
       return
    end if

    ! get number of fields in a colon delimited string list
    nflds = 0
    if (len_trim(str) > 0) then
       count = 0
       do n = 1, len_trim(str)
          if (str(n:n) == ':') count = count + 1
       end do
       nflds = count + 1
    endif

    ! allocate memory for flds)
    allocate(flds(nflds))

    do k = 1,nflds
       ! start with whole list
       i0 = 1
       i1 = len_trim(str)

       ! remove field names before kth field
       do n = 2,k
          i = index(str(i0:i1),':')
          i0 = i0 + i
       end do

       ! remove field names after kth field
       if (k < nFlds) then
          i = index(str(i0:i1),':')
          i1 = i0 + i - 2
       end if

       ! set flds(k)
       flds(k) = str(i0:i1)//"   "
    end do

  end subroutine med_phases_history_get_auxflds

  !===============================================================================
  subroutine med_phases_history_output_alarminfo(mclock, alarm, alarmname, rc)

    ! input/output variables
    type(ESMF_Clock), intent(in)  :: mclock
    type(ESMF_Alarm), intent(in)  :: alarm
    character(len=*), intent(in)  :: alarmname
    integer         , intent(out) :: rc

    ! local variables
    type(ESMF_TimeInterval) :: ringInterval
    integer                 :: ringInterval_length
    type(ESMF_Time)         :: currtime
    type(ESMF_Time)         :: nexttime
    character(len=CS)       :: currtimestr
    character(len=CS)       :: nexttimestr
    integer                 :: yr,mon,day,sec ! time units
    character(len=*), parameter :: subname='(med_phases_history_output_alarminfo)'
    !---------------------------------------

    rc = ESMF_SUCCESS

    call ESMF_AlarmGet(alarm, ringInterval=ringInterval, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_TimeIntervalGet(ringInterval, s=ringinterval_length, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_ClockGet(mclock, currtime=currtime, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_TimeGet(currtime,yy=yr, mm=mon, dd=day, s=sec, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    write(currtimestr,'(i4.4,a,i2.2,a,i2.2,a,i5.5)') yr,'-',mon,'-',day,'-',sec
    call ESMF_ClockGetNextTime(mclock, nextTime=nexttime, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    call ESMF_TimeGet(nexttime, yy=yr, mm=mon, dd=day, s=sec, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return
    if (mastertask) then
       write(nexttimestr,'(i4.4,a,i2.2,a,i2.2,a,i5.5)') yr,'-',mon,'-',day,'-',sec
       write(logunit,*)
       write(logunit,'(a,i8)') trim(subname)//": history alarmname "//trim(alarmname)//&
            ' is ringing, interval length is ', ringInterval_length            
       write(logunit,'(a)') trim(subname)//": currtime = "//trim(currtimestr)//" nexttime = "//trim(nexttimestr)
    end if

  end subroutine med_phases_history_output_alarminfo

  !===============================================================================
  subroutine med_phases_history_ymds2rday_offset(currtime, rdays_offset, &
       years_offset, months_offset, days_offset, seconds_offset, rc)

    ! Given the current time and optional year, month, day and seconds offsets
    ! from the current time: Return an offset from the current time given in fractional days.
    ! For example, if day_offset = -2 and seconds_offset = -21600, rday_offset will be -2.25.
    ! One or more of the following optional arguments should be provided:

    ! input/output variables
    type(ESMF_Time) , intent(in)           :: currtime       ! current time
    real(r8)        , intent(out)          :: rdays_offset   ! offset from current time in fractional days
    integer         , intent(in), optional :: years_offset   ! number of years offset from current time
    integer         , intent(in), optional :: months_offset  ! number of months offset from current time
    integer         , intent(in), optional :: days_offset    ! number of days offset from current time
    integer         , intent(in), optional :: seconds_offset ! number of seconds offset from current time
    integer         , intent(out)          :: rc

    ! local variables
    type(ESMF_TimeInterval) :: timeinterval
    !---------------------------------------

    rc = ESMF_SUCCESS

    call ESMF_TimeIntervalSet(timeinterval=timeinterval, startTime=currtime, &
         YY=years_offset, MM=months_offset, D=days_offset, S=seconds_offset, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

    call ESMF_TimeIntervalGet(timeinterval=timeinterval, d_r8=rdays_offset, rc=rc)
    if (ChkErr(rc,__LINE__,u_FILE_u)) return

  end subroutine med_phases_history_ymds2rday_offset

end module med_phases_history_mod
