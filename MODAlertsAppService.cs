using Abp.Application.Services.Dto;
using Abp.Authorization;
using Abp.Data;
using Abp.Domain.Uow;
using Abp.Linq.Extensions;
using Abp.MultiTenancy;
using FS.Authorization;
using FS.Authorization.Users.Enums;
using FS.EntityFrameworkCore;
using FS.FSEntity.Alert.Dtos;
using FS.FSEntity.Alert.Enums;
using FS.FSEntity.Dtos;
using FS.FSEntity.Form;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using System;
using System.Globalization;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Net;
using System.Threading.Tasks;
using FS.FSEntity.Report;
using Abp.Runtime.Security;
using Abp.Timing;
using FS.Authorization.Users.Dto;
using Abp.Runtime.Session;
using System.Linq.Dynamic.Core;
using FS.Extensions;
using Hangfire;
using FS.FSEntity.Hangfire;
using System.Net.Mail;
using FS.Configuration.Host.Dto;
using System.IO;
using FS.Dto;
using System.Net.Mime;
using Abp.Net.Mail;

namespace FS.FSEntity.Alert
{
    [AbpAuthorize(AppPermissions.Pages_Alerts)]
    public partial class AlertsAppService : FSAppServiceBase, IAlertsAppService
    {
        private string _session_GeneratedAlert = "ALERT_GENERATED_";
        private string _session_GeneratedAlertDetails = "ALERT_GENERATED_DETAILS_";

        public async Task<PagedResultDto<GetAlertForViewDtoCustom>> GetAllAlert(GetAllAlertsInputCustom input)
        {
            var filteredAlerts = _alertRepository.GetAll()
                        .WhereIf(!string.IsNullOrWhiteSpace(input.Filter), e => false || e.Name.Contains(input.Filter) || e.EmailSubject.Contains(input.Filter) || e.EmailBody.Contains(input.Filter) || e.QueryStatement.Contains(input.Filter) || e.JSONStatement.Contains(input.Filter) || e.DashboardTitle.Contains(input.Filter));

            var query = from o in filteredAlerts
                        orderby o.Id descending
                        select new GetAlertForViewDtoCustom
                        {
                            Alert = ObjectMapper.Map<AlertDto>(o),
                            CreateDate = o.CreationTime,
                            //IsExpired = o.NatureTypeID == (int)AlertNatureEnum.Never_Expire ? false : (o.EndDate <= DateTime.UtcNow ? true : false)
                        };

            var totalCount = await query.CountAsync();

            var alerts = await query
                .OrderBy(input.Sorting ?? "alert.id desc")
                .PageBy(input)
                .ToListAsync();

            return new PagedResultDto<GetAlertForViewDtoCustom>(
                totalCount,
                alerts
            );
        }

        [AbpAuthorize(AppPermissions.Pages_Alerts_Edit)]
        public async Task<GetAlertForEditOutputCustom> GetAlertForEditCustom(EntityDto<long> input)
        {
            GetAlertForEditOutputCustom output = new GetAlertForEditOutputCustom();
            Alert alert = await _alertRepository.GetAsync(input.Id);
            output.Alert = ObjectMapper.Map<CustomCreateOrEditAlertDto>(alert);

            if(!alert.IsSendToAll)
            {
                output.Recievers = ObjectMapper.Map<List<AlertRecieverDto>>(
                    _alertRecieverRepository.GetAll().Where(e => !e.IsDeleted
                    && e.AlertID == alert.Id));
            }

            if(alert.GenerateTypeID == (int)AlertGeneratedType.Query_Builder)
            {
                output.QueryFields = ObjectMapper.Map<List<AlertDetailDto>>(
                    _alertDetailRepository.GetAll().Where(e => !e.IsDeleted
                    && e.AlertID == alert.Id));
            }

            return output;
        }

        public async Task<List<AlertDetailDto>> GetAllAlertDetail(GetAlertDetailInput input)
        {
            List<AlertDetailDto> output = null;
            output = ObjectMapper.Map<List<AlertDetailDto>>(
                 _alertDetailRepository.GetAll().Where(e => !e.IsDeleted
                 && e.AlertID == input.AlertID
                 && e.PageID == input.PageID
                 ));

            await Task.CompletedTask;

            return output;
        }

        public async Task<AlertQueryDto> GetAlertQuery(long alert)
        {
            Alert objAlert = await _alertRepository.GetAsync(alert);
            AlertQueryDto output = JsonConvert.DeserializeObject<AlertQueryDto>(objAlert.JSONStatement);
            output = output ?? new AlertQueryDto();

            return output;
        }

        public async Task<GetAllAlertSytemLookup> GetAllAlertSytemLookup()
        {
            GetAllAlertSytemLookup output = new GetAllAlertSytemLookup();

            SystemLookup systemLookup = null;

            IEnumerable<SystemLookup> nature = null;
            IEnumerable<SystemLookup> frequency = null;
            IEnumerable<SystemLookup> contentType = null;
            IEnumerable<SystemLookup> userType = null;
            IEnumerable<SystemLookup> alerType = null;

            systemLookup = _systemLookupRepository.GetAll().Where(e => e.Key == "AlertNatureType" && e.ParentID == 0).FirstOrDefault();
            nature = _systemLookupRepository.GetAll().Where(e => e.ParentID == systemLookup.Id);
            output.Nature = ObjectMapper.Map<List<SystemLookupDto>>(nature);
            nature = null;
            systemLookup = null;

            systemLookup = _systemLookupRepository.GetAll().Where(e => e.Key == "FrequencyType" && e.ParentID == 0).FirstOrDefault();
            frequency = _systemLookupRepository.GetAll().Where(e => e.ParentID == systemLookup.Id);
            output.Frequency = ObjectMapper.Map<List<SystemLookupDto>>(frequency);
            frequency = null;
            systemLookup = null;

            systemLookup = _systemLookupRepository.GetAll().Where(e => e.Key == "AlertGeneratedType" && e.ParentID == 0).FirstOrDefault();
            contentType = _systemLookupRepository.GetAll().Where(e => e.ParentID == systemLookup.Id);
            output.ContentType = ObjectMapper.Map<List<SystemLookupDto>>(contentType);
            contentType = null;
            systemLookup = null;

            systemLookup = _systemLookupRepository.GetAll().Where(e => e.Key == "UserType" && e.ParentID == 0).FirstOrDefault();
            userType = _systemLookupRepository.GetAll().Where(e => e.ParentID == systemLookup.Id);
            output.RecieverUserType = ObjectMapper.Map<List<SystemLookupDto>>(userType);
            userType = null;
            systemLookup = null;

            systemLookup = _systemLookupRepository.GetAll().Where(e => e.Key == "AlertType" && e.ParentID == 0).FirstOrDefault();
            if (systemLookup != null) {
                alerType = _systemLookupRepository.GetAll().Where(e => e.ParentID == systemLookup.Id);
                output.AlertType = ObjectMapper.Map<List<SystemLookupDto>>(alerType);
                alerType = null;
                systemLookup = null;
            }

            await Task.CompletedTask;

            return output;
        }

        public async Task<IEnumerable<PageLookupTableDto>> GetAllPageLookup()
        {
            // Get the roles of the logged in user 
            var roleIds = await _userRoleRepository.GetAll()
                    .Join(_roleRepository.GetAll(),
                        ur => ur.RoleId,
                        r => r.Id,
                        (userRole, role) => new { UserRole = userRole, Role = role })
                    .Where(x => x.UserRole.UserId == AbpSession.ToUserIdentifier().UserId)
                    .Select(x => (long)x.Role.Id).ToListAsync();

            var grouped = (from p in _pageRepository.GetAll().Where(e => !e.IsDeleted).Where(e => e.Name != "CV Information")
                           join i in _pageFormRepository.GetAll().Where(e => !e.IsDeleted) on p.Id equals i.PageID
                           group p by new { p.Id, p.Name, p.Status, p.Default } into g
                           select new { g.Key.Id, g.Key.Name, g.Key.Status, g.Key.Default, TotalForm = g.Count() }
               );

            var output = await (from o in grouped
                          join p in _permissionDataAccessRepository.GetAll().Where(x => x.RecordReport && roleIds.Contains(x.RoleID)) on o.Id equals p.PageID
                          select new PageLookupTableDto()
                          {
                              Id = o.Id,
                              DisplayName = o.Name
                          }).Distinct().ToListAsync();

            return output;

        }

        public async Task<IEnumerable<PageFormLookupTableDto>> GetAllPageFormLookup(long page)
        {
            List<PageFormLookupTableDto> output = new List<PageFormLookupTableDto>();

            var user = UserManager.GetUser(AbpSession.ToUserIdentifier());

            // Get the roles of the logged in user 
            var roleIds = _userRoleRepository.GetAll()
                    .Join(_roleRepository.GetAll(),
                        ur => ur.RoleId,
                        r => r.Id,
                        (userRole, role) => new { UserRole = userRole, Role = role })
                    .Where(x => x.UserRole.UserId == user.Id)
                    .Select(x => (long)x.Role.Id).ToList();


            // This should return all report field property that has isReport = true as per logged in user role
            var permissions = _permissionDataAccessRepository.GetAll()
                .Include(p => p.Page)
                .Include(p => p.Form)
                .Include(p => p.FormField)
                .Where(p => p.PageID == page && roleIds.Contains(p.RoleID) && p.RecordReport)
                .ToList();

            var isReportableFields = permissions.Select(x => x.FormField.Id).ToList();
            foreach (var form in permissions.Select(s => s.Form).Distinct())
            {
                output.Add(
                    new PageFormLookupTableDto
                    {
                        Id = form.Id,
                        DisplayName = form.Name,
                        FormFields = _formFieldRepository.GetAll()
                                .Include(x => x.FormMasterField)
                                .Include("FormMasterField.FieldType")
                                .Where(t => !t.IsDeleted && t.Status && t.FormID == form.Id && isReportableFields.Contains(t.Id))
                                .OrderBy(t => t.OrderID)
                                .Select(t => new PageFormFieldLookupTableDto
                                {
                                    Id = t.Id,
                                    FieldType = t.FormMasterField.FieldType.Value,
                                    DisplayName = string.IsNullOrEmpty(t.DisplayName) || string.IsNullOrWhiteSpace(t.DisplayName) ? $"[{_formMasterFieldRepository.Get(t.FormMasterFieldID).FieldName}]" : t.DisplayName
                                }).ToList()
                    }
                    );
            }

            await Task.CompletedTask;

            return output.Distinct();
        }

        public async Task<IEnumerable<ReportLookupTableDto>> GetAllSavedReportLookup()
        {
            IEnumerable<ReportLookupTableDto> output = _reportRepository.GetAll()
                .Where(e => !e.IsDeleted && e.Status)
                .Select(e => new ReportLookupTableDto { Id = e.Id, DisplayName = e.Name }).ToList();

            await Task.CompletedTask;

            return output;
        }

        public async Task<IEnumerable<Dtos.UserLookupTableDto>> GetAllUserLookup()
        {
            IEnumerable<Dtos.UserLookupTableDto> output = _userRepository.GetAll()
                .Where(e => !e.IsDeleted && e.IsActive && (e.UserType == (int)UserType.FendleyStaff || e.Id == (long)AbpSession.UserId))
                .Select(e => new Dtos.UserLookupTableDto { Id = e.Id, DisplayName = e.Name.Decrypt() + " " + e.Surname.Decrypt() }).ToList();

            await Task.CompletedTask;

            return output;
        }

        public async Task<IEnumerable<Dtos.UserLookupTableDto>> GetAllUserLookupByUserType(List<UserType> userType)
        {
            IEnumerable<Dtos.UserLookupTableDto> output = _userRepository.GetAll()
                .Where(e => !e.IsDeleted && e.IsActive && (userType.Contains((UserType)e.UserType) || e.Id == (long)AbpSession.UserId))
                .Select(e => new Dtos.UserLookupTableDto { Id = e.Id, DisplayName = e.Name.Decrypt() + " " + e.Surname.Decrypt() }).ToList();

            await Task.CompletedTask;

            return output;
        }

        public async Task<SaveAlertResponse> Save(SaveAlertDto input)
        {
            SaveAlertResponse output = new SaveAlertResponse();
            Alert alert = null;
            AlertDetail alertDetail = null;
            AlertReciever alertReciever;

            bool proceedSaving = false;
            bool isNewRecord = input.Id < 1;

            try
            {
                #region Validation
                input.Name = input.Name == null ? string.Empty : input.Name.Trim();
                if (string.IsNullOrEmpty(input.Name))
                {
                    output.Messages.Add("Please provide Alert Name.");
                }

                if (input.StartDate.Date < DateTime.Now.Date && isNewRecord)
                {
                    output.Messages.Add("Start Date cannot be less than Current Date.");
                }

                switch (input.NatureTypeID)
                {
                    case (int)AlertNatureEnum.Specific_Date:
                        if (!input.EndDate.HasValue)
                        {
                            output.Messages.Add("Please provide End Date.");
                        }
                        else
                        {
                            if (input.EndDate.Value < input.StartDate)
                            {
                                output.Messages.Add("End Date cannot be less than Start Date.");
                            }
                            else if (input.EndDate.Value < DateTime.Now.Date && isNewRecord)
                            {
                                output.Messages.Add("End Date cannot be less than Current Date.");
                            }
                        }
                        break;
                    case (int)AlertNatureEnum.Never_Expire:
                        input.EndDate = null;
                        break;
                    default:
                        output.Messages.Add("Please provide Alert Nature.");
                        break;
                }

                switch (input.FrequencyID)
                {
                    case (int)FrequencyType.Weekly:
                    case (int)FrequencyType.Bi_Weekly:
                        if (input.DayOfWeek < 1 || input.DayOfWeek > 7)
                        {
                            output.Messages.Add("Please choose Day of the Week.");
                        }
                        break;
                    case (int)FrequencyType.Daily:
                    case (int)FrequencyType.Monthly:
                        input.DayOfWeek = 0;
                        break;
                    default:
                        output.Messages.Add("Please provide Alert Frequency.");
                        break;
                }

                switch (input.AlertTypeId)
                {
                    case (int)AlertType.Passport_Expiry:
                        break;
                    case (int)AlertType.Immigration_Expiry:
                        break;
                    case (int)AlertType.Contract_Expiry:
                        break;
                    case (int)AlertType.Certification_Expiry:
                        break;
                    case (int)AlertType.Insurance_Expiry:
                        break;
                    default:
                        output.Messages.Add("Please provide alert type.");
                        break;
                }

                switch (input.GenerateTypeID)
                {
                    case (int)AlertGeneratedType.Saved_Report:
                        input.DashboardTitle = string.Empty;
                        if (input.AlertToID != (int)UserType.FendleyStaff)
                        {
                            output.Messages.Add("Saved Reports can only be sent to Fendley Staff.");
                        }

                        if (!input.SendinEmail)
                        {
                            output.Messages.Add("Saved Reports can only be sent thru Email.");
                        }
                        input.SendinDashboard = false;

                        if (input.ReportID < 1)
                        {
                            output.Messages.Add("Please provide Saved Report.");
                        }

                        input.Recievers = input.Recievers ?? new List<AlertRecieverDto>();
                        if (!input.IsSendToAll && !input.Recievers.Any())
                        {
                            output.Messages.Add("Please provide Fendley Staff.");
                        }

                        if (input.Recievers.Where(e => e.UserID == 0).Any())
                        {
                            input.IsSendToAll = true;
                            input.Recievers.Clear();
                        }
                        break;
                    case (int)AlertGeneratedType.Query_Builder:
                        // Maynard 17/03/2020: Old code sends all alert to fendly staff if generate type 
                        // is query builder. I am not seeing this logic on BRD. Why?
                        //input.IsSendToAll = true;

                        input.DashboardTitle = input.DashboardTitle == null ? string.Empty : input.DashboardTitle.Trim();
                        if (string.IsNullOrEmpty(input.DashboardTitle))
                        {
                            output.Messages.Add("Please provide Dashboard Title.");
                        }

                        if (!input.AlertDetails.Any())
                        {
                            output.Messages.Add("Please provide at least 1 Field.");
                        }

                        if (!input.HasNoCondition && !input.AlertQuery.QueryConditionsFields.Any())
                        {
                            output.Messages.Add("Please provide at least 1 condition.");
                        }
                        break;
                    default:
                        output.Messages.Add("Please provide Alert to Generate.");
                        break;
                }

                input.EmailSubject = input.EmailSubject == null ? string.Empty : input.EmailSubject.Trim();
                if (string.IsNullOrEmpty(input.EmailSubject) && input.SendinEmail)
                {
                    output.Messages.Add("Please provide Email Subject.");
                }

                input.EmailBody = input.EmailBody == null ? string.Empty : input.EmailBody.Trim();
                if (string.IsNullOrEmpty(input.EmailBody) && input.SendinEmail)
                {
                    output.Messages.Add("Please provide Email Body.");
                }

                proceedSaving = output.Messages.Count == 0;
                #endregion

                if (proceedSaving)
                {
                    if (!isNewRecord)
                    {
                        alert = _alertRepository.Get(input.Id);
                    }

                    alert = alert ?? new Alert();
                    alert.Name = input.Name;
                    alert.NatureTypeID = input.NatureTypeID;
                    alert.StartDate = input.StartDate;
                    if (input.EndDate != null)
                        alert.EndDate = input.EndDate.Value;
                    alert.FrequencyID = input.FrequencyID;
                    alert.AlertTypeID = input.AlertTypeId;
                    alert.DayOfWeek = input.DayOfWeek;
                    alert.GenerateTypeID = input.GenerateTypeID;
                    alert.AlertToID = input.AlertToID;
                    alert.SendinEmail = input.SendinEmail;
                    alert.SendinDashboard = input.SendinDashboard;
                    alert.Status = input.Status;
                    alert.ReportID = input.ReportID;
                    alert.DashboardTitle = input.DashboardTitle;
                    alert.EmailSubject = input.EmailSubject;
                    alert.EmailBody = input.EmailBody;
                    alert.IsSendToAll = input.IsSendToAll;

                    if (alert.GenerateTypeID == (int)AlertGeneratedType.Saved_Report)
                    {
                        alert.JSONStatement = string.Empty;
                        alert.QueryStatement = string.Empty;
                    }
                    else if (alert.GenerateTypeID == (int)AlertGeneratedType.Query_Builder)
                    {
                        alert.QueryStatement = BuildQuery(input).ToString();
                        alert.JSONStatement = JsonConvert.SerializeObject(input.AlertQuery);
                    }

                    if (isNewRecord)
                    {
                        //save alert and get id
                        alert.Id = _alertRepository.InsertAndGetId(alert);
                    }
                    else
                    {
                        if (alert.Id < 1)
                        {
                            output.Messages.Add("Alert doest not exists.");
                        }
                        else
                        {
                            await _alertRepository.UpdateAsync(alert);
                        }
                    }

                    string CronExpression = "";
                    var schedtime = input.StartDate.ToUniversalTime().ToString("hh-mm").Split("-");
                    switch (input.FrequencyID)
                    {
                        case (int)FrequencyType.Daily:
                        {
                            CronExpression = Cron.Daily(int.Parse(schedtime[0]), int.Parse(schedtime[1]));
                            break;
                        }
                        case (int)FrequencyType.Weekly:
                        {
                            CronExpression = Cron.Weekly((DayOfWeek)input.DayOfWeek, int.Parse(schedtime[0]), int.Parse(schedtime[1]));
                            break;
                        }
                        case (int)FrequencyType.Bi_Weekly:
                        {
                            CronExpression = string.Format("*/{0} */{1} */{2} * *", int.Parse(schedtime[1]), int.Parse(schedtime[0]), 15);
                            break;
                        }
                        case (int)FrequencyType.Monthly:
                        {
                            CronExpression = Cron.Monthly(1, int.Parse(schedtime[0]), int.Parse(schedtime[1]));
                            break;
                        }
                        default:
                        {
                            CronExpression = Cron.Yearly(1, 1, int.Parse(schedtime[0]), int.Parse(schedtime[1]));
                            break;
                        }
                    }

                    //create hang fire job using newly created id
                    //string.Format("{0}-{1}", alert.Id, alert.Name.Replace(" ", ""))
                    RecurringJob.AddOrUpdate<AlertBackgroundJobs>(alert.Id.ToString(), s => s.Execute(alert.Id), CronExpression);

                    // Check if there are existing receivers
                    IQueryable<AlertReciever> existingAlertRecievers = _alertRecieverRepository.GetAll().Where(e => !e.IsDeleted
                            && e.AlertID == alert.Id);


                    if (!alert.IsSendToAll)
                    {
                        if (existingAlertRecievers != null)
                        {
                            var nonSelectedReceivers = existingAlertRecievers.Where(x => !input.Recievers.Select(y => y.UserID).Contains(x.UserID));

                            // Delete non-selected users ids
                            foreach (AlertReciever existingAlertReciever in nonSelectedReceivers)
                            {
                                await _alertRecieverRepository.DeleteAsync(existingAlertReciever);
                            }
                        }

                        foreach (AlertRecieverDto reciever in input.Recievers)
                        {
                            alertReciever = _alertRecieverRepository.GetAll().Where(e => !e.IsDeleted
                            && e.AlertID == alert.Id
                            && e.UserID == reciever.UserID).FirstOrDefault();

                            alertReciever = alertReciever ?? new AlertReciever();
                            alertReciever.AlertID = alert.Id;
                            alertReciever.UserID = reciever.UserID;

                            if (alertReciever.Id < 1)
                            {
                                await _alertRecieverRepository.InsertAsync(alertReciever);
                            }
                            else
                            {
                                await _alertRecieverRepository.UpdateAsync(alertReciever);
                            }

                            alertReciever = null;
                        }
                    } else {                        

                        if (existingAlertRecievers != null)
                        {
                            foreach (AlertReciever existingAlertReciever in existingAlertRecievers)
                            {
                                await _alertRecieverRepository.DeleteAsync(existingAlertReciever);
                            }
                        }
                    }

                    #region old code
                    /*
					if (alert.GenerateTypeID == (int)AlertGeneratedType.Saved_Report)
                    {
                        if (!alert.IsSendToAll)
                        {
                            foreach (AlertRecieverDto reciever in input.Recievers)
                            {
                                alertReciever = _alertRecieverRepository.GetAll().Where(e => !e.IsDeleted
                                && e.AlertID == alert.Id
                                && e.UserID == reciever.UserID).FirstOrDefault();

                                alertReciever = alertReciever ?? new AlertReciever();
                                alertReciever.AlertID = alert.Id;
                                alertReciever.UserID = reciever.UserID;

                                if (alertReciever.Id < 1)
                                {
                                    await _alertRecieverRepository.InsertAsync(alertReciever);
                                }
                                else
                                {
                                    await _alertRecieverRepository.UpdateAsync(alertReciever);
                                }

                                alertReciever = null;
                            }
                        }
                        else
                        {
                            IQueryable<AlertReciever> existingAlertRecievers = _alertRecieverRepository.GetAll().Where(e => !e.IsDeleted
                                && e.AlertID == alert.Id);

                            if (existingAlertRecievers != null)
                            {
                                foreach (AlertReciever existingAlertReciever in existingAlertRecievers)
                                {
                                    await _alertRecieverRepository.DeleteAsync(existingAlertReciever);
                                }
                            }
                        }
                    }
                    else */
                    #endregion

                    if (alert.GenerateTypeID == (int)AlertGeneratedType.Query_Builder)
                    {
                        // If Alert is not new record delete Alert Detail
                        if (!isNewRecord)
                        {
                            var existingAlertDetails = _alertDetailRepository.GetAll()
                                    .Where(e => !e.IsDeleted
                                    && !input.AlertDetails.Select(t => t.PageID).Contains(e.PageID)
                                    && !input.AlertDetails.Select(t => t.FormID).Contains(e.FormID)
                                    && !input.AlertDetails.Select(t => t.FormFieldID).Contains(e.FormFieldID)
                                    && e.AlertID == alert.Id
                                    );
                            foreach (var existingAlertDetail in existingAlertDetails)
                            {
                                await _alertDetailRepository.DeleteAsync(existingAlertDetail);
                            }
                        }

                        foreach (AlertDetailDto queryField in input.AlertDetails)
                        {
                            alertDetail = _alertDetailRepository.GetAll()
                                .Where(e => !e.IsDeleted
                                && e.PageID == queryField.PageID
                                && e.FormID == queryField.FormID
                                && e.FormFieldID == queryField.FormFieldID
                                && e.AlertID == alert.Id
                            ).FirstOrDefault();

                            alertDetail = alertDetail ?? new AlertDetail();
                            alertDetail.PageID = queryField.PageID;
                            alertDetail.FormID = queryField.FormID;
                            alertDetail.FormFieldID = queryField.FormFieldID;
                            alertDetail.AlertID = alert.Id;

                            if(alertDetail.Id == 0 || isNewRecord)
                            {
                                alertDetail.CreatorUserId = (long)AbpSession.UserId;
                                _alertDetailRepository.Insert(alertDetail);
                            }
                            else
                            {
                                _alertDetailRepository.Update(alertDetail);
                            }

                            alertDetail = null;
                        }
                    }

                    output.IsSuccess = true;
                    output.Messages.Add(L("SavedSuccessfully"));
                }
                else
                {
                    output.IsSuccess = false;
                }
            }
            catch (Exception ex)
            {
                output.IsSuccess = false;
                output.Messages.Add(ex.Message);
                output.Messages.Add(ex.InnerException.ToString());
            }

            return output;
        }

        public async Task<string> GeneratePreview(GenerateAlertQueryInput input)
        {
            string output = string.Empty;
            DataTable result = new DataTable();
            DataRow dataRow = null;
            string query = string.Empty;
            SaveAlertDto saveAlertDto = new SaveAlertDto
            {
                AlertQuery = input.AlertQuery,
                AlertDetails = input.AlertDetails,
                HasNoCondition = input.HasNoCondition,
                PageID = input.PageID
            };
            query = BuildQuery(saveAlertDto).ToString();

            List<FormField> formFields = _formFieldRepository.GetAll().Where(e => input.AlertDetails.Select(t => t.FormFieldID).Contains(e.Id)).OrderBy(e => e.OrderID).ToList();
            List<string> columns = new List<string>();
            string columnName = string.Empty;

            List<long> allowedFields = new List<long>();
            // Filter columns as per permission if not super admin or impersonating
            if (!isSuperAdmin(AbpSession.UserId ?? 0) || _impersonator.IsLoginAsUser())
            {
                allowedFields = getFormFieldsWithPermission(AbpSession.UserId ?? 0);
            }

            foreach (FormField field in formFields)
            {
                // do not add alert columns
                if (allowedFields.Any()) if (!allowedFields.Contains(field.Id)) continue;

                columnName = field.DisplayName == string.Empty ? $"[{field.FormMasterField.FieldName}]" : field.DisplayName;
                if (!result.Columns.Contains(columnName))
                {
                    result.Columns.Add(columnName);
                }
                else
                {
                    int sameColumnCtr = 1;
                    columnName += $"{sameColumnCtr}";
                    while (result.Columns.Contains(columnName))
                    {
                        columnName += $"{++sameColumnCtr}";
                    }
                    result.Columns.Add(columnName);
                }

                columns.Add(columnName);
            }

            DbCommand command = _dbContextProvider.GetDbContext().Database.GetDbConnection().CreateCommand();
            command.CommandText =await ReplaceQueryToken(query, AbpSession.UserId);
            command.CommandType = CommandType.Text;
            command.Transaction = (DbTransaction)_activeTransactionProvider.GetActiveTransaction(new ActiveTransactionProviderArgs()
            {
                {"ContextType", typeof(FSDbContext) },
                {"MultiTenancySide", MultiTenancySides.Host }
            });

            List<GetAlertResultDto> alertResultDtos = new List<GetAlertResultDto>();
            GetAlertResultDto alertResultDto = null;
            using (DbDataReader reader = await command.ExecuteReaderAsync())
            {
                while (reader.Read())
                {
                    alertResultDto = new GetAlertResultDto
                    {
                        Form = "" + reader[0],
                        Field = "" + reader[1],
                        Value = "" + reader[2],
                        UserID = long.Parse("" + reader[3]),
                        GroupInputId = long.Parse("" + reader[4])
                    };
                    alertResultDtos.Add(alertResultDto);
                    alertResultDto = null;
                }

                foreach (long userID in alertResultDtos.Select(e => e.UserID).Distinct())
                {
                    dataRow = result.NewRow();
                    List<string> inputDataFields = new List<string>();
                    foreach (var item in alertResultDtos.Where(e => e.UserID == userID))
                    {
                        dataRow[item.Field] = item.Value;
                        inputDataFields.Add(item.Field);
                    }

                    if (inputDataFields.Count < result.Columns.Count)
                    {
                        foreach (var column in result.Columns.Cast<DataColumn>())
                        {
                            if (inputDataFields.IndexOf(column.ColumnName) == -1)
                            {
                                dataRow[column.ColumnName] = string.Empty;
                            }
                        }
                    }

                    result.Rows.Add(dataRow);
                    dataRow = null;
                }
            }

            output = Guid.NewGuid().ToString("N");
            _tempObjects.UpdateTemp(columns, _session_GeneratedAlertDetails + output);
            _tempObjects.UpdateTemp(result, _session_GeneratedAlert + output);

            await Task.CompletedTask;

            return output;
        }

        public async Task<DataTable> GetGeneratedPreview(string input)
        {
            List<string> columns = await _tempObjects.GetTemp<List<string>>(_session_GeneratedAlertDetails + input);
            DataTable output = await _tempObjects.GetTemp<DataTable>(_session_GeneratedAlert + input);
            output = output ?? new DataTable();

            if (output.Rows.Count == 0)
            {
                foreach (string column in columns)
                {
                    output.Columns.Add(column);
                }
            }

            return output;
        }


        [AbpAllowAnonymous]
        public async Task<DataTable> GenerateAlert(long alert)
        {
            DataTable output = new DataTable();
            DataRow dataRow = null;
            string query = string.Empty;

            Alert objAlert = await _alertRepository.GetAsync(alert);

            // Update query to filter result by user id permission
            query = await ReplaceQueryToken(objAlert.QueryStatement, AbpSession.UserId);

            var alertDetails = new List<AlertDetail>();

            List<FormField> formFields = new List<FormField>();

            using (_unitOfWorkManager.Current.DisableFilter(AbpDataFilters.SoftDelete))
            {
                alertDetails = _alertDetailRepository.GetAllList().Where(e => !e.IsDeleted && e.AlertID == alert).ToList();
                formFields = _formFieldRepository.GetAllList().Where(e => alertDetails.Select(t => t.FormFieldID).Contains(e.Id)).OrderBy(e => e.OrderID).ToList();
            }

            List<string> columns = new List<string>();
            string columnName = string.Empty;

            List<long> allowedFields = new List<long>();
            // Filter columns as per permission if not super admin or impersonating
            if (!isSuperAdmin(AbpSession.UserId ?? 0) || _impersonator.IsLoginAsUser())
            {
                allowedFields = getFormFieldsWithPermission(AbpSession.UserId ?? 0);
            }

            foreach (FormField field in formFields)
            {
                // do not add alert columns
                if (allowedFields.Any()) if (!allowedFields.Contains(field.Id)) continue;

                columnName = field.DisplayName == string.Empty ? $"[{field.FormMasterField.FieldName}]" : field.DisplayName;
                if (!output.Columns.Contains(columnName))
                {
                    output.Columns.Add(columnName);
                }
                else
                {
                    int sameColumnCtr = 1;
                    columnName += $"{sameColumnCtr}";
                    while (output.Columns.Contains(columnName))
                    {
                        columnName += $"{++sameColumnCtr}";
                    }
                    output.Columns.Add(columnName);
                }

                columns.Add(columnName);
            }

            DbCommand command = _dbContextProvider.GetDbContext().Database.GetDbConnection().CreateCommand();
            command.CommandText = query;
            command.CommandType = CommandType.Text;
            command.Transaction = (DbTransaction)_activeTransactionProvider.GetActiveTransaction(new ActiveTransactionProviderArgs()
            {
                {"ContextType", typeof(FSDbContext) },
                {"MultiTenancySide", MultiTenancySides.Host }
            });

            List<GetAlertResultDto> alertResultDtos = new List<GetAlertResultDto>();
            GetAlertResultDto alertResultDto = null;
            using (DbDataReader reader = await command.ExecuteReaderAsync())
            {
                while (reader.Read())
                {
                    alertResultDto = new GetAlertResultDto
                    {
                        Form = "" + reader[0],
                        Field = "" + reader[1],
                        Value = "" + reader[2],
                        UserID = long.Parse("" + reader[3]),
                        GroupInputId = long.Parse("" + reader[4])
                    };
                    alertResultDtos.Add(alertResultDto);
                    alertResultDto = null;
                }

                foreach (long userID in alertResultDtos.Select(e => e.UserID).Distinct())
                {
                    dataRow = output.NewRow();
                    List<string> inputDataFields = new List<string>();
                    foreach (var item in alertResultDtos.Where(e => e.UserID == userID))
                    {
                        dataRow[item.Field] = item.Value;
                        inputDataFields.Add(item.Field);
                    }

                    if (inputDataFields.Count < output.Columns.Count)
                    {
                        foreach (var column in output.Columns.Cast<DataColumn>())
                        {
                            if (inputDataFields.IndexOf(column.ColumnName) == -1)
                            {
                                dataRow[column.ColumnName] = string.Empty;
                            }
                        }
                    }

                    output.Rows.Add(dataRow);
                    dataRow = null;
                }
            }

            return output;
        }

        public async Task<PagedResultDto<AlertDashboarDto>> GetCertificateExpiry()
        {
            List<AlertDashboarDto> output = new List<AlertDashboarDto>();
            AlertDashboarDto dto = null;
            bool enableAlert = bool.Parse(SettingManager.GetSettingValueAsync(FSSettingsLookup.Alert_Enable).Result);

            if (enableAlert)
            {
                DateTime utcNow = Clock.Now.ToUniversalTime();

                IEnumerable<Certificate.Certificate> certificates = _certificateRepository.GetAll().Where(e => !e.IsDeleted && e.Expiry != null && e.Expiry <= utcNow.AddDays(60));
                certificates = certificates ?? new List<Certificate.Certificate>();

                DateTime expiry;
                bool sendAlert;
                foreach (Certificate.Certificate certificate in certificates)
                {
                    sendAlert = false;
                    expiry = certificate.Expiry.Value;

                    if (expiry.Date == utcNow.Date) // expired today
                    {
                        sendAlert = true;
                    }
                    else
                    {
                        int dateDiff = (utcNow.Date - expiry.Date).Days;
                        if (dateDiff % 14 == 0 || dateDiff % 14 == 4)
                        {
                            sendAlert = true;
                        }
                    }

                    if (sendAlert)
                    {
                        dto = new AlertDashboarDto();
                        dto.Date = expiry.Date;
                        dto.AlertText = L("CertificationExpiringInNext60Days");
                        dto.AlertType = L("CertificationExpiry");
                        output.Add(dto);
                        dto = null;
                    }
                }
            }

            await Task.CompletedTask;

            //Fore testing
            //dto = new AlertDashboarDto();
            //dto.ExpiryDate = DateTime.Now;
            //dto.AlertText = "" + L("CertificationExpiringInNext60Days");
            //dto.AlertType = L("TestOnly");
            //output.Add(dto);
            //dto = null;

            return new PagedResultDto<AlertDashboarDto>(
                output.Count(),
                output
            );
        }

        public async Task<PagedResultDto<AlertDashboarDto>> GetAlertForToday()
        {
            List<AlertDashboarDto> output = new List<AlertDashboarDto>();
            AlertDashboarDto dto = null;
            bool enableAlert = bool.Parse(SettingManager.GetSettingValueForApplicationAsync(FSSettingsLookup.Alert_Enable).Result);

            if (enableAlert)
            {
                DateTime utcNow = Clock.Now.ToUniversalTime();

                IEnumerable<Alert> alerts = _alertRepository.GetAll().Where(e => !e.IsDeleted && e.Status
                    && e.SendinDashboard
                    && e.StartDate <= utcNow
                );

                alerts = alerts ?? new List<Alert>();

                bool proceed = false;
                foreach (Alert alert in alerts)
                {
                    proceed = false;
                    switch (alert.NatureTypeID)
                    {
                        case (long)AlertNatureEnum.Specific_Date:
                            if (alert.EndDate >= utcNow.Date)
                            {
                                proceed = true;
                            }
                            break;
                        case (long)AlertNatureEnum.Never_Expire:
                            proceed = true;
                            break;
                        default:
                            break;
                    }

                    if (proceed)
                    {
                        switch (alert.FrequencyID)
                        {
                            case (long)FrequencyType.Daily:
                                proceed = true;
                                break;
                            case (long)FrequencyType.Weekly:
                            case (long)FrequencyType.Bi_Weekly:
                                if (alert.DayOfWeek == ((int)utcNow.DayOfWeek - 1))
                                {
                                    proceed = true;
                                }
                                break;
                            case (long)FrequencyType.Monthly:
                                if (alert.StartDate.Day == utcNow.Day)
                                {
                                    proceed = true;
                                }
                                break;
                            default:
                                proceed = false;
                                break;
                        }

                        if (proceed)
                        {
                            dto = new AlertDashboarDto();
                            dto.AlertId = alert.Id;
                            dto.Date = utcNow;
                            dto.AlertText = alert.DashboardTitle;
                            dto.AlertType = L("CreatedAlert");
                            output.Add(dto);
                            dto = null;
                        }
                    }
                }
            }

            await Task.CompletedTask;

            return new PagedResultDto<AlertDashboarDto>(
               output.Count(),
               output
           );
        }

        [AbpAllowAnonymous]
        public async Task<List<AlertDashboarDto>> ViewDashboardAlerts(GetAlertDashboardInput input)
        {
            List<AlertDashboarDto> output = new List<AlertDashboarDto>();

            DateTime utcNow = Clock.Now.ToUniversalTime();
            AlertDashboarDto dto = null;
            bool proceed = false;
            DateTime expiry;
            long formId;
            long formFielId;

            Authorization.Users.User currentUser = await UserManager.FindByIdAsync(AbpSession.GetUserId().ToString());
            List<UserListDto> users = await GetUsersByOrgGroup();
            UserListDto user = null;
            bool isManager = await UserManager.IsInRoleAsync(currentUser, "Manager");

            string alertType = input.Type.ToUpper().Split("_")[0];
            switch (alertType)
            {
                case "PASSPORT":
                    IEnumerable<FormFieldData> passports = null;
                    formId = _formRepository.GetAll().Where(e => e.Name == "Passport").FirstOrDefault().Id;
                    formFielId = _formFieldRepository.GetAll().Where(e => e.FormID == formId && e.DisplayName == "Expiry").FirstOrDefault().Id;

                    if (currentUser.Id == 2)
                    {
                        passports = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId);
                    }
                    else if (currentUser.Id != 2 && isManager)
                    {
                        passports = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                        && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
                    }

                    passports = passports ?? new List<FormFieldData>();

                    foreach (FormFieldData passportData in passports)
                    {
                        string passport = passportData.Value;
                        if (!string.IsNullOrEmpty(passport))
                        {
#if DEBUG                           
                            expiry = DateTime.ParseExact(passport, "dd/MM/yyyy", new CultureInfo("en-AU"));
#else
                            expiry = Convert.ToDateTime($"{passport.Split('/')[1]}/{passport.Split('/')[0]}/{passport.Split('/')[2]}");
#endif

                            if (expiry <= utcNow.AddDays(60))
                            {
                                proceed = false;
                                if (expiry.Date == utcNow.Date) // expired today
                                {
                                    proceed = true;
                                }
                                else
                                {
                                    int dateDiff = (expiry.Date - utcNow.Date).Days;
                                    if (dateDiff % 30 == 0)
                                    {
                                        proceed = true;
                                    }
                                }

                                if (proceed)
                                {
                                    user = users.Where(e => e.Id == passportData.UserID).FirstOrDefault();

                                    dto = new AlertDashboarDto
                                    {
                                        AlertGroup = "EXPIRY",
                                        Date = expiry.Date,
                                        Name = user == null || user.Id == 0 ? currentUser.FullName : user.FullName,
                                        AlertText = L("PassportExpiringInNext60Days"),
                                        AlertType = "PASSPORT"
                                    };
                                    output.Add(dto);
                                    dto = null;
                                }
                            }
                        }
                    }
                    break;
                case "IMMIGRATION":
                    IEnumerable<FormFieldData> immigrations = null;
                    formId = _formRepository.GetAll().Where(e => e.Name == "Visa").FirstOrDefault().Id;
                    formFielId = _formFieldRepository.GetAll().Where(e => e.FormID == formId && e.DisplayName == "Expiry").FirstOrDefault().Id;

                    if (currentUser.Id == 2)
                    {
                        immigrations = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId);
                    }
                    else if (currentUser.Id != 2 && isManager)
                    {
                        immigrations = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                        && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
                    }

                    immigrations = immigrations ?? new List<FormFieldData>();

                    foreach (FormFieldData immigrationData in immigrations)
                    {
                        string immigration = immigrationData.Value;
                        if (!string.IsNullOrEmpty(immigration))
                        {
#if DEBUG                            
                            expiry = DateTime.ParseExact(immigration, "dd/MM/yyyy", new CultureInfo("en-AU"));
#else
                            expiry = Convert.ToDateTime($"{immigration.Split('/')[1]}/{immigration.Split('/')[0]}/{immigration.Split('/')[2]}");
#endif

                            if (expiry <= utcNow.AddDays(60))
                            {
                                proceed = false;
                                if (expiry.Date == utcNow.Date) // expired today
                                {
                                    proceed = true;
                                }
                                else
                                {
                                    int dateDiff = (expiry.Date - utcNow.Date).Days;
                                    if (dateDiff % 30 == 0)
                                    {
                                        proceed = true;
                                    }
                                }

                                if (proceed)
                                {
                                    user = users.Where(e => e.Id == immigrationData.UserID).FirstOrDefault();

                                    dto = new AlertDashboarDto
                                    {
                                        AlertGroup = "EXPIRY",
                                        Date = expiry.Date,
                                        Name = user == null || user.Id == 0 ? currentUser.FullName : user.FullName,
                                        AlertText = L("ImmigrationExpiringInNext60Days"),
                                        AlertType = "IMMIGRATION"
                                    };
                                    output.Add(dto);
                                    dto = null;
                                }
                            }
                        }
                    }
                    break;
                case "CONTRACT":
                    IEnumerable<FormFieldData> contracts = null;
                    formId = _formRepository.GetAll().Where(e => e.Name == "Contract").FirstOrDefault().Id;
                    formFielId = _formFieldRepository.GetAll().Where(e => e.FormID == formId && e.DisplayName == "Expiry").FirstOrDefault().Id;

                    if (currentUser.Id == 2)
                    {
                        contracts = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId);
                    }
                    else if (currentUser.Id != 2 && isManager)
                    {
                        contracts = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                        && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
                    }

                    contracts = contracts ?? new List<FormFieldData>();

                    foreach (FormFieldData contractData in contracts)
                    {
                        string contract = contractData.Value;
                        if (!string.IsNullOrEmpty(contract))
                        {
#if DEBUG                           
                            expiry = DateTime.ParseExact(contract, "dd/MM/yyyy", new CultureInfo("en-AU"));
#else
                            expiry = Convert.ToDateTime($"{contract.Split('/')[1]}/{contract.Split('/')[0]}/{contract.Split('/')[2]}");
#endif

                            if (expiry <= utcNow.AddDays(60))
                            {
                                proceed = false;
                                if (expiry.Date == utcNow.Date) // expired today
                                {
                                    proceed = true;
                                }
                                else
                                {
                                    int dateDiff = (expiry.Date - utcNow.Date).Days;
                                    if (dateDiff % 30 == 0)
                                    {
                                        proceed = true;
                                    }
                                }

                                if (proceed)
                                {
                                    user = users.Where(e => e.Id == contractData.UserID).FirstOrDefault();

                                    dto = new AlertDashboarDto
                                    {
                                        AlertGroup = "EXPIRY",
                                        Date = expiry.Date,
                                        Name = user == null || user.Id == 0 ? currentUser.FullName : user.FullName,
                                        AlertText = L("ContractExpiringInNext60Days"),
                                        AlertType = "CONTRACT"
                                    };
                                    output.Add(dto);
                                    dto = null;
                                }
                            }
                        }
                    }
                    break;
                case "INSURANCES":
                    IEnumerable<FormFieldData> insurances = null;
                    formId = _formRepository.GetAll().Where(e => e.Name == "Insurance").FirstOrDefault().Id;
                    formFielId = _formFieldRepository.GetAll().Where(e => e.FormID == formId && e.DisplayName == "Expiry").FirstOrDefault().Id;

                    if (currentUser.Id == 2)
                    {
                        insurances = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId);
                    }
                    else if (currentUser.Id != 2 && isManager)
                    {
                        insurances = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                        && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
                    }

                    insurances = insurances ?? new List<FormFieldData>();

                    foreach (FormFieldData insuranceData in insurances)
                    {
                        string insurance = insuranceData.Value;
                        if (!string.IsNullOrEmpty(insurance))
                        {
#if DEBUG                           
                            expiry = DateTime.ParseExact(insurance, "dd/MM/yyyy", new CultureInfo("en-AU"));
#else
                            expiry = Convert.ToDateTime($"{insurance.Split('/')[1]}/{insurance.Split('/')[0]}/{insurance.Split('/')[2]}");
#endif

                            if (expiry <= utcNow.AddDays(60))
                            {
                                proceed = false;
                                if (expiry.Date == utcNow.Date) // expired today
                                {
                                    proceed = true;
                                }
                                else
                                {
                                    int dateDiff = (expiry.Date - utcNow.Date).Days;
                                    if (dateDiff % 30 == 0)
                                    {
                                        proceed = true;
                                    }
                                }

                                if (proceed)
                                {
                                    user = users.Where(e => e.Id == insuranceData.UserID).FirstOrDefault();

                                    dto = new AlertDashboarDto
                                    {
                                        AlertGroup = "EXPIRY",
                                        Date = expiry.Date,
                                        Name = user == null || user.Id == 0 ? currentUser.FullName : user.FullName,
                                        AlertText = L("InsurancesExpiringInNext60Days"),
                                        AlertType = "INSURANCES"
                                    };
                                    output.Add(dto);
                                    dto = null;
                                }
                            }
                        }
                    }
                    break;
                case "CONTACT":
                    IEnumerable<FormFieldData> contacts = null;
                    formId = _formRepository.GetAll().Where(e => e.Name == "Person Detail").FirstOrDefault().Id;
                    List<long> formFielIds = _formFieldRepository.GetAll().Where(e => e.FormID == formId).Select(e => e.Id).ToList();

                    if (currentUser.Id == 2)
                    {
                        insurances = _formFieldDataRepository.GetAll().Where(e => formFielIds.Contains(e.FormFieldID));
                    }
                    else if (currentUser.Id != 2 && isManager)
                    {
                        insurances = _formFieldDataRepository.GetAll().Where(e => formFielIds.Contains(e.FormFieldID)
                        && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
                    }

                    contacts = contacts ?? new List<FormFieldData>();

                    foreach (FormFieldData data in contacts)
                    {
                        expiry = data.CreationTime;

                        if (data.LastModificationTime.HasValue)
                        {
                            expiry = data.LastModificationTime.Value;
                        }

                        if (expiry <= utcNow.AddDays(180))
                        {
                            proceed = false;
                            if (expiry.Date == utcNow.Date) // expired today
                            {
                                proceed = true;
                            }
                            else
                            {
                                int dateDiff = (expiry.Date - utcNow.Date).Days;
                                if (dateDiff % 180 == 0)
                                {
                                    proceed = true;
                                }
                            }

                            if (proceed)
                            {
                                user = users.Where(e => e.Id == data.UserID).FirstOrDefault();

                                dto = new AlertDashboarDto
                                {
                                    AlertGroup = "UPDATE",
                                    Date = expiry.Date,
                                    Name = user == null || user.Id == 0 ? currentUser.FullName : user.FullName,
                                    UserId = user == null || user.Id == 0 ? currentUser.Id : user.Id,
                                    AlertText = L("Contact180daysUpdateNotification"),
                                    AlertType = "CONTACT"
                                };

                                output.RemoveAll(e => e.UserId == dto.UserId);
                                output.Add(dto);
                                dto = null;
                            }
                        }
                    }
                    break;
                case "CERTIFICATION":
                    IEnumerable<Certificate.Certificate> certificates = null;

                    if (currentUser.Id == 2)
                    {
                        certificates = _certificateRepository.GetAll().Where(e => !e.IsDeleted && e.Expiry != null && e.Expiry <= utcNow.AddDays(60));
                    }
                    else if (currentUser.Id != 2 && isManager)
                    {
                        certificates = _certificateRepository.GetAll().Where(e => !e.IsDeleted && e.Expiry != null
                        && e.Expiry <= utcNow.AddDays(60)
                        && (users.Select(t => t.Id).Contains((long)e.CreatorUserId) || e.CreatorUserId == currentUser.Id));
                    }

                    certificates = certificates ?? new List<Certificate.Certificate>();

                    foreach (Certificate.Certificate certificate in certificates)
                    {
                        proceed = false;
                        expiry = certificate.Expiry.Value;

                        if (expiry.Date == utcNow.Date) // expired today
                        {
                            proceed = true;
                        }
                        else
                        {
                            int dateDiff = (expiry.Date - utcNow.Date).Days;
                            if (dateDiff % 14 == 0 || dateDiff % 14 == 4)
                            {
                                proceed = true;
                            }
                        }

                        if (proceed)
                        {
                            user = users.Where(e => e.Id == certificate.CreatorUserId).FirstOrDefault();

                            dto = new AlertDashboarDto
                            {
                                AlertGroup = "EXPIRY",
                                Date = expiry.Date,
                                Name = user == null || user.Id == 0 ? currentUser.FullName : user.FullName,
                                AlertText = L("CertificationExpiringInNext60Days"),
                                AlertType = "CERTIFICATE"
                            };
                            output.Add(dto);
                            dto = null;
                            break;
                        }
                    }
                    break;
                default:
                    break;
            }

            return output;
        }

        [AbpAllowAnonymous]
        public async Task<GetAlertDashboard> GetDashboardAlerts()
        {
            GetAlertDashboard output = new GetAlertDashboard();
            output.SystemAlert = new List<AlertDashboarDto>();
            output.CustomizeAlert = new List<AlertDashboarDto>();

            DateTime utcNow = Clock.Now.ToUniversalTime();
            AlertDashboarDto dto = null;
            DateTime expiry;
            long formId;
            long formFielId;
            bool proceed = false;

            Authorization.Users.User currentUser = await UserManager.FindByIdAsync(AbpSession.GetUserId().ToString());
            List<UserListDto> users = await GetUsersByOrgGroup();
            bool isManager = await UserManager.IsInRoleAsync(currentUser, "Manager");
            output.IsNormalUser = !isManager && currentUser.Id != 2;

            #region "System Alert"
            #region "Passport"

            #region "Passport System Generated"

            IEnumerable<FormFieldData> passports = null;
            var form = _formRepository.GetAll().Where(e => e.Name == "Passport").FirstOrDefault();
            formId = form != null ? form.Id : 0;
            var formfield = _formFieldRepository.GetAll().Where(e => e.FormID == formId && e.DisplayName == "Expiry").FirstOrDefault();
            formFielId = formfield != null ? formfield.Id : 0;

            if (currentUser.Id == 2)
            {
                passports = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId);
            }
            else if (currentUser.Id != 2 && isManager)
            {
                passports = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
            }
            else
            {
                passports = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                && e.UserID == currentUser.Id);
            }

            passports = passports ?? new List<FormFieldData>();



            foreach (string passport in passports.Select(e => e.Value).Distinct())
            {
                if (!string.IsNullOrEmpty(passport))
                {
#if DEBUG                   
                    expiry = DateTime.ParseExact(passport, "dd/MM/yyyy", new CultureInfo("en-AU"));
#else
                    expiry = Convert.ToDateTime($"{passport.Split('/')[1]}/{passport.Split('/')[0]}/{passport.Split('/')[2]}");
#endif

                    if (expiry <= utcNow.AddDays(60))
                    {
                        proceed = false;
                        if (expiry.Date == utcNow.Date) // expired today
                        {
                            proceed = true;
                        }
                        else
                        {
                            int dateDiff = (expiry.Date - utcNow.Date).Days;
                            if (dateDiff % 30 == 0)
                            {
                                proceed = true;
                            }
                        }

                        if (proceed)
                        {
                            dto = new AlertDashboarDto();
                            dto.AlertGroup = "EXPIRY";
                            dto.Date = expiry.Date;
                            dto.AlertText = L("PassportExpiringInNext60Days");
                            dto.AlertType = Enum.GetName(typeof(AlertType), AlertType.Passport_Expiry);
                            output.SystemAlert.Add(dto);
                            dto = null;
                            break;
                        }
                    }
                }
            }

            #endregion "Passport System Generated"

            #region "Passport Alert Expiry"

            var passExpiryAlerts = GetExpiryAlerts(AlertType.Passport_Expiry);

            if (passExpiryAlerts.Any()) output.SystemAlert.AddRange(passExpiryAlerts);

            #endregion Passport Alert Expiry

            #endregion "Passport"

            #region "Immigration"

            #region "Immigration System Generated"

            IEnumerable<FormFieldData> immigrations = null;
            form = _formRepository.GetAll().Where(e => e.Name == "Visa").FirstOrDefault();
            formId = form != null ? form.Id : 0;
            formfield = _formFieldRepository.GetAll().Where(e => e.FormID == formId && e.DisplayName == "Expiry").FirstOrDefault();
            formFielId = formfield != null ? formfield.Id : 0;

            if (currentUser.Id == 2)
            {
                immigrations = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId);
            }
            else if (currentUser.Id != 2 && isManager)
            {
                immigrations = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
            }
            else
            {
                immigrations = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                && e.UserID == currentUser.Id);
            }

            immigrations = immigrations ?? new List<FormFieldData>();

            foreach (string immigration in immigrations.Select(e => e.Value).Distinct())
            {
                if (!string.IsNullOrEmpty(immigration))
                {
#if DEBUG                   
                    expiry = DateTime.ParseExact(immigration, "dd/MM/yyyy", new CultureInfo("en-AU"));
#else
                    expiry = Convert.ToDateTime($"{immigration.Split('/')[1]}/{immigration.Split('/')[0]}/{immigration.Split('/')[2]}");
#endif

                    if (expiry <= utcNow.AddDays(60))
                    {
                        proceed = false;
                        if (expiry.Date == utcNow.Date) // expired today
                        {
                            proceed = true;
                        }
                        else
                        {
                            int dateDiff = (expiry.Date - utcNow.Date).Days;
                            if (dateDiff % 30 == 0)
                            {
                                proceed = true;
                            }
                        }

                        if (proceed)
                        {
                            dto = new AlertDashboarDto();
                            dto.AlertGroup = "EXPIRY";
                            dto.Date = expiry.Date;
                            dto.AlertText = L("ImmigrationExpiringInNext60Days");
                            dto.AlertType = Enum.GetName(typeof(AlertType), AlertType.Immigration_Expiry);
                            output.SystemAlert.Add(dto);
                            dto = null;
                            break;
                        }
                    }
                }
            }

            #endregion "Immigration System Generated"

            #region "Immigration Alert Expiry"

            var ImmiExpiryAlerts = GetExpiryAlerts(AlertType.Immigration_Expiry);

            if (ImmiExpiryAlerts.Any()) output.SystemAlert.AddRange(ImmiExpiryAlerts);

            #endregion Immigration Alert Expiry

            #endregion "Immigration"

            #region "Contract"     

            #region "Contract System Generated" 

            IEnumerable<FormFieldData> contracts = null;
            form = _formRepository.GetAll().Where(e => e.Name == "Contract").FirstOrDefault();
            formId = form != null ? form.Id : 0;
            formfield = _formFieldRepository.GetAll().Where(e => e.FormID == formId && e.DisplayName == "Expiry").FirstOrDefault();
            formFielId = formfield != null ? formfield.Id : 0;

            if (currentUser.Id == 2)
            {
                contracts = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId);
            }
            else if (currentUser.Id != 2 && isManager)
            {
                contracts = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
            }
            else
            {
                contracts = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                && e.UserID == currentUser.Id);
            }

            contracts = contracts ?? new List<FormFieldData>();

            foreach (string contract in contracts.Select(e => e.Value).Distinct())
            {
                if (!string.IsNullOrEmpty(contract))
                {
#if DEBUG
                    //expiry = Convert.ToDateTime(contract);
                    expiry = DateTime.ParseExact(contract, "dd/MM/yyyy", new CultureInfo("en-AU"));
#else
                    expiry = Convert.ToDateTime($"{contract.Split('/')[1]}/{contract.Split('/')[0]}/{contract.Split('/')[2]}");
#endif                    

                    if (expiry <= utcNow.AddDays(60))
                    {
                        proceed = false;
                        if (expiry.Date == utcNow.Date) // expired today
                        {
                            proceed = true;
                        }
                        else
                        {
                            int dateDiff = (expiry.Date - utcNow.Date).Days;
                            if (dateDiff % 30 == 0)
                            {
                                proceed = true;
                            }
                        }

                        if (proceed)
                        {
                            dto = new AlertDashboarDto();
                            dto.AlertGroup = "EXPIRY";
                            dto.Date = expiry.Date;
                            dto.AlertText = L("ContractExpiringInNext60Days");
                            dto.AlertType = Enum.GetName(typeof(AlertType), AlertType.Contract_Expiry);
                            output.SystemAlert.Add(dto);
                            dto = null;
                            break;
                        }
                    }
                }
            }

            #endregion "Contract System Generated" 

            #region "Contract Alert Expiry"

            var contractExpiryAlerts = GetExpiryAlerts(AlertType.Contract_Expiry);

            if (contractExpiryAlerts.Any()) output.SystemAlert.AddRange(contractExpiryAlerts);

            #endregion Contract Alert Expiry

            #endregion "Contract"

            #region "Insurances"        

            #region "Insurances System Generated"  
            IEnumerable<FormFieldData> insurances = null;
            form = _formRepository.GetAll().Where(e => e.Name == "Insurance").FirstOrDefault();
            formId = form != null ? form.Id : 0;
            formfield = _formFieldRepository.GetAll().Where(e => e.FormID == formId && e.DisplayName == "Expiry").FirstOrDefault();
            formFielId = formfield != null ? formfield.Id : 0;

            if (currentUser.Id == 2)
            {
                insurances = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId);
            }
            else if (currentUser.Id != 2 && isManager)
            {
                insurances = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
            }
            else
            {
                insurances = _formFieldDataRepository.GetAll().Where(e => e.FormFieldID == formFielId
                && e.UserID == currentUser.Id);
            }

            insurances = insurances ?? new List<FormFieldData>();

            foreach (string insurance in insurances.Select(e => e.Value).Distinct())
            {
                if (!string.IsNullOrEmpty(insurance))
                {
#if DEBUG
                    expiry =  DateTime.ParseExact(insurance, "dd/MM/yyyy", new CultureInfo("en-AU"));
#else
                    expiry = Convert.ToDateTime($"{insurance.Split('/')[1]}/{insurance.Split('/')[0]}/{insurance.Split('/')[2]}");
#endif

                    if (expiry <= utcNow.AddDays(60))
                    {
                        proceed = false;
                        if (expiry.Date == utcNow.Date) // expired today
                        {
                            proceed = true;
                        }
                        else
                        {
                            int dateDiff = (expiry.Date - utcNow.Date).Days;
                            if (dateDiff % 30 == 0)
                            {
                                proceed = true;
                            }
                        }

                        if (proceed)
                        {
                            dto = new AlertDashboarDto();
                            dto.AlertGroup = "EXPIRY";
                            dto.Date = expiry.Date;
                            dto.AlertText = L("InsurancesExpiringInNext60Days");
                            dto.AlertType = Enum.GetName(typeof(AlertType), AlertType.Insurance_Expiry);
                            output.SystemAlert.Add(dto);
                            dto = null;
                            break;
                        }
                    }
                }
            }
            #endregion "Insurances System Generated"  

            #region "Insurances Alert Expiry"

            var insurancesExpiryAlerts = GetExpiryAlerts(AlertType.Insurance_Expiry);

            if (insurancesExpiryAlerts.Any()) output.SystemAlert.AddRange(insurancesExpiryAlerts);

            #endregion Insurances Alert Expiry

            #endregion "Insurances"

            #region "Certificate"         

            #region "Certificate System Generated"  
            IEnumerable<Certificate.Certificate> certificates = null;

            if (currentUser.Id == 2)
            {
                certificates = _certificateRepository.GetAll().Where(e => !e.IsDeleted && e.Expiry != null && e.Expiry <= utcNow.AddDays(60));
            }
            else if (currentUser.Id != 2 && isManager)
            {
                certificates = _certificateRepository.GetAll().Where(e => !e.IsDeleted && e.Expiry != null
                && e.Expiry <= utcNow.AddDays(60)
                && (users.Select(t => t.Id).Contains((long)e.CreatorUserId) || e.CreatorUserId == currentUser.Id));
            }
            else
            {
                certificates = _certificateRepository.GetAll().Where(e => !e.IsDeleted && e.Expiry != null
                && e.Expiry <= utcNow.AddDays(60)
                && e.CreatorUserId == currentUser.Id);
            }

            certificates = certificates ?? new List<Certificate.Certificate>();

            foreach (DateTime? certificate in certificates.Select(e => e.Expiry).Distinct())
            {
                proceed = false;
                expiry = certificate.Value;

                if (expiry.Date == utcNow.Date) // expired today
                {
                    proceed = true;
                }
                else
                {
                    int dateDiff = (expiry.Date - utcNow.Date).Days;
                    if (dateDiff % 14 == 0 || dateDiff % 14 == 4)
                    {
                        proceed = true;
                    }
                }

                if (proceed)
                {
                    dto = new AlertDashboarDto();
                    dto.AlertGroup = "EXPIRY";
                    dto.Date = expiry.Date;
                    dto.AlertText = L("CertificationExpiringInNext60Days");
                    dto.AlertType = Enum.GetName(typeof(AlertType), AlertType.Certification_Expiry);
                    output.SystemAlert.Add(dto);
                    dto = null;
                    break;
                }
            }
            #endregion "Certificate System Generated" 

            #region "Certificate Alert Expiry"

            var certificateExpiryAlerts = GetExpiryAlerts(AlertType.Certification_Expiry);

            if (certificateExpiryAlerts.Any()) output.SystemAlert.AddRange(certificateExpiryAlerts);

            #endregion Certificate Alert Expiry

            #endregion "Certificate"

            #region "Old code Contact"
            /*
            IEnumerable<FormFieldData> contacts = null;
            form = _formRepository.GetAll().Where(e => e.Name == "Person Detail").FirstOrDefault();
            formId = form != null ? form.Id : 0;
            List<long> formFielIds = _formFieldRepository.GetAll().Where(e => e.FormID == formId).Select(e => e.Id).ToList();

            if (currentUser.Id == 2)
            {
                contacts = _formFieldDataRepository.GetAll().Where(e => formFielIds.Contains(e.FormFieldID));
            }
            else if (currentUser.Id != 2 && isManager)
            {
                contacts = _formFieldDataRepository.GetAll().Where(e => formFielIds.Contains(e.FormFieldID)
                && (users.Select(t => t.Id).Contains(e.UserID) || e.UserID == currentUser.Id));
            }
            else
            {
                contacts = _formFieldDataRepository.GetAll().Where(e => formFielIds.Contains(e.FormFieldID)
                && e.UserID == currentUser.Id);
            }

            contacts = contacts ?? new List<FormFieldData>();

            foreach (FormFieldData formFieldData in contacts)
            {
                expiry = formFieldData.CreationTime;
                if (formFieldData.LastModificationTime.HasValue)
                {
                    expiry = formFieldData.LastModificationTime.Value;
                }

                if (expiry <= utcNow.AddDays(180))
                {
                    proceed = false;
                    if (expiry.Date == utcNow.Date) // expired today
                    {
                        proceed = true;
                    }
                    else
                    {
                        int dateDiff = (expiry.Date - utcNow.Date).Days;
                        if (dateDiff % 180 == 0)
                        {
                            proceed = true;
                        }
                    }

                    if (proceed)
                    {
                        dto = new AlertDashboarDto();
                        dto.AlertGroup = "UPDATE";
                        dto.Date = expiry.Date;
                        dto.AlertText = L("Contact180daysUpdateNotification");
                        dto.AlertType = "CONTACT";
                        output.SystemAlert.Add(dto);
                        dto = null;
                        break;
                    }
                }
            }
            */
            #endregion "Contact"

            #endregion


            #region "Old code - Custom Alert"
            /*
            var alerts = new List<Alert>();
            if (currentUser.Id == 2)
            {
                alerts = _alertRepository
                    .GetAll()
                    .Where(e => !e.IsDeleted
                        && e.Status
                        && e.SendinDashboard
                        && e.StartDate.ToUniversalTime() >= utcNow)
                    .ToList();
            }
            else
            {
                alerts = _alertRepository
                    .GetAll()
                    .Where(e => !e.IsDeleted
                        && e.Status
                        && e.SendinDashboard
                        && e.StartDate.ToUniversalTime() <= utcNow
                        && e.AlertToID == currentUser.UserType)
                    .ToList();
            }

            foreach (Alert alert in alerts)
            {
                proceed = false;
                switch (alert.NatureTypeID)
                {
                    case (long)AlertNatureEnum.Specific_Date:
                        if (alert.EndDate.HasValue && alert.EndDate.Value.ToUniversalTime() >= utcNow)
                            proceed = true;
                        break;
                    case (long)AlertNatureEnum.Never_Expire:
                    default:
                        proceed = true;
                        break;
                }

                if (proceed)
                {
                    if (!alert.IsSendToAll)
                    {
                        proceed = _alertRecieverRepository
                            .GetAllList()
                            .Where(e => !e.IsDeleted && e.AlertID == alert.Id && e.UserID == currentUser.Id)
                            .Any();
                    }

                    if (proceed)
                    {
                        switch (alert.FrequencyID)
                        {
                            case (long)FrequencyType.Daily:
                                proceed = true;
                                break;
                            case (long)FrequencyType.Weekly:
                            case (long)FrequencyType.Bi_Weekly:
                                if (alert.DayOfWeek == ((int)utcNow.DayOfWeek - 1))
                                    proceed = true;
                                break;
                            case (long)FrequencyType.Monthly:
                                if (alert.StartDate.ToUniversalTime().Day == utcNow.Day)
                                    proceed = true;
                                break;
                            default:
                                proceed = false;
                                break;
                        }

                        if (proceed)
                        {
                            dto = new AlertDashboarDto
                            {
                                AlertId = alert.Id,
                                Date = utcNow,
                                AlertText = alert.DashboardTitle
                            };
                            output.CustomizeAlert.Add(dto);
                            dto = null;
                        }
                    }
                }
            }
            */
            #endregion

            return output;
        }

        [AbpAllowAnonymous]
        public async Task SendAlert(long alertId)
        {
            #region 1) Get Alert

            Alert alert = _alertRepository.FirstOrDefault(a => a.Id == alertId);
            if (alert == null) return; // Nothing to process

            #endregion

            #region 2) Update the alert that it's in progress

            alert.IsInProgress = true;
            alert.LastStartDateTime = DateTime.Now;
            _alertRepository.Update(alert);

            #endregion

            #region 3 Get query and form fields

            string query = string.Empty;
            List<FormField> formFields = new List<FormField>();
            if (alert.GenerateTypeID == (long)AlertGeneratedType.Query_Builder)
            {
                query = alert.QueryStatement;
                IEnumerable<AlertDetail> alertDetails = _alertDetailRepository.GetAllList().Where(e => !e.IsDeleted && e.AlertID == alertId);
                formFields = _formFieldRepository.GetAllList().Where(e => alertDetails.Select(t => t.FormFieldID).Contains(e.Id)).OrderBy(e => e.OrderID).ToList();
            }
            else if (alert.GenerateTypeID == (long)AlertGeneratedType.Saved_Report && alert.ReportID.HasValue)
            {
                Report.Report report = _reportRepository.FirstOrDefault(r => r.Id == alert.ReportID);
                if (report != null)
                {
                    query = report.QueryStatement;
                    IEnumerable<ReportDetail> reportDetails = _reportDetailRepository.GetAllList().Where(e => !e.IsDeleted && e.ReportID == report.Id);
                    formFields = _formFieldRepository.GetAllList().Where(e => reportDetails.Select(t => t.FormFieldID).Contains(e.Id)).OrderBy(e => e.OrderID).ToList();
                }
            }

            #endregion


            #region 5) Get the receivers and send the email

            var recievers = Enumerable.Empty<object>()
                          .Select(r => new { EmailAddress = "", Id = 0L });

            if (alert.GenerateTypeID == (long)AlertGeneratedType.Saved_Report)
            {
                if (alert.IsSendToAll)
                {
                    recievers = _userRepository.GetAllList()
                        .Where(e => !e.IsDeleted && e.IsActive && e.UserType == (int)UserType.FendleyStaff)
                        .Select(e => new { e.EmailAddress, e.Id });
                }
                else
                {
                    List<long> alertRecievers = _alertRecieverRepository.GetAllList().Where(e => !e.IsDeleted && e.AlertID == alert.Id).Select(e => e.UserID).ToList();
                    recievers = _userRepository.GetAllList()
                        .Where(e => !e.IsDeleted && e.IsActive && alertRecievers.Contains(e.Id))
                        .Select(e => new { e.EmailAddress, e.Id });
                }
            }
            else if (alert.GenerateTypeID == (long)AlertGeneratedType.Query_Builder)
            {
                if (alert.IsSendToAll)
                {
                    recievers = _userRepository.GetAllList()
                        .Where(e => !e.IsDeleted && e.IsActive && e.UserType == alert.AlertToID)
                       .Select(e => new { e.EmailAddress, e.Id });
                }
                else
                {
                    List<long> alertRecievers = _alertRecieverRepository.GetAllList().Where(e => !e.IsDeleted && e.AlertID == alert.Id).Select(e => e.UserID).ToList();
                    recievers = _userRepository.GetAllList()
                        .Where(e => !e.IsDeleted && e.IsActive && alertRecievers.Contains(e.Id))
                        .Select(e => new { e.EmailAddress, e.Id });
                }
            }

            EmailSettingsEditDto emailSettings = await GetEmailSettingsAsync();
            Attachment emailAttachment = null;
            //emailSettings.SmtpUseDefaultCredentials 
            SmtpClient mailServer = new SmtpClient(emailSettings.SmtpHost, emailSettings.SmtpPort)
            {
                EnableSsl = emailSettings.SmtpEnableSsl,
                UseDefaultCredentials = emailSettings.SmtpUseDefaultCredentials,
                Credentials = new NetworkCredential(emailSettings.SmtpUserName, emailSettings.SmtpPassword)
            };
            try
            {
                foreach (var reciever in recievers)
                {

                    #region 4) Get form fields columns and data, then export the file

                    List<string> columns = new List<string>();
                    DataTable output = new DataTable();
                    DataRow dataRow = null;
                    string columnName = string.Empty;
                    List<long> allowedFields = new List<long>();
                    // Filter columns as per permission if not super admin or impersonating
                    if (!isSuperAdmin(reciever.Id) || _impersonator.IsLoginAsUser())
                    {
                        allowedFields = getFormFieldsWithPermission(reciever.Id);
                    }

                    foreach (FormField field in formFields)
                    {
                        // do not add alert columns
                        if (allowedFields.Any()) if (!allowedFields.Contains(field.Id)) continue;

                        columnName = field.DisplayName == string.Empty ? $"[{field.FormMasterField.FieldName}]" : field.DisplayName;
                        if (!output.Columns.Contains(columnName))
                        {
                            output.Columns.Add(columnName);
                        }
                        else
                        {
                            int sameColumnCtr = 1;
                            columnName += $"{sameColumnCtr}";
                            while (output.Columns.Contains(columnName))
                            {
                                columnName += $"{++sameColumnCtr}";
                            }
                            output.Columns.Add(columnName);
                        }

                        columns.Add(columnName);
                    }

                    if (!string.IsNullOrEmpty(query))
                    {
                        // Update query to filter result by user id permission
                        query = await ReplaceQueryToken(query, reciever.Id);
                        List<GetAlertResultDto> alertResultDtos = ExecuteAlertQuery(query);
                        foreach (long userId in alertResultDtos.Select(e => e.UserID).Distinct())
                        {
                            dataRow = output.NewRow();
                            List<string> inputDataFields = new List<string>();
                            foreach (var item in alertResultDtos.Where(e => e.UserID == userId))
                            {
                                dataRow[item.Field] = item.Value;
                                inputDataFields.Add(item.Field);
                            }

                            if (inputDataFields.Count < output.Columns.Count)
                            {
                                foreach (var column in output.Columns.Cast<DataColumn>())
                                {
                                    if (inputDataFields.IndexOf(column.ColumnName) == -1)
                                    {
                                        dataRow[column.ColumnName] = string.Empty;
                                    }
                                }
                            }
                            output.Rows.Add(dataRow);
                            dataRow = null;
                        }
                    }

                    FileDto file = _alertsExcelExporter.GenerateAlertExportToFile(output);
                    byte[] alertFile = _tempFileCacheManager.GetFile(file.FileToken);
                    emailAttachment = new Attachment(new MemoryStream(alertFile), $"{alert.Name}.xlsx", MediaTypeNames.Application.Octet);
                    #endregion


                    MailMessage message = new MailMessage(
                        emailSettings.DefaultFromAddress,
                        reciever.EmailAddress,
                        alert.EmailSubject,
                        alert.EmailBody)
                    {
                        IsBodyHtml = true
                    };
                    // Add the file attachment to this email message.
                    message.Attachments.Add(emailAttachment);
                    //Send the message.
                    await mailServer.SendMailAsync(message);

                    alert.IsInProgress = false;
                    alert.LastEndDateTime = DateTime.UtcNow;
                    _alertRepository.Update(alert);
                }

            }
            finally
            {
                emailAttachment?.Dispose();
                mailServer?.Dispose();
            }

            #endregion
        }

        [AbpAllowAnonymous]
        public async Task SendManualAlert(long alertId)
        {
            #region 1) Get Alert

            Alert alert = _alertRepository.FirstOrDefault(a => a.Id == alertId);
            if (alert == null) return; // Nothing to process

            #endregion

            #region 2) Update the alert that it's in progress 

            // If the alert is set to show on dashboard, update the StartDate to Now, so it shows up on the dashboard (temporary fix)
            if (alert.SendinDashboard)
                alert.StartDate = Clock.Now;

            alert.IsInProgress = true;
            alert.LastStartDateTime = DateTime.Now;
            _alertRepository.Update(alert);

            #endregion

            #region 3 Get query and form fields

            string query = string.Empty;
            List<FormField> formFields = new List<FormField>();
            if (alert.GenerateTypeID == (long)AlertGeneratedType.Query_Builder)
            {
                query = alert.QueryStatement;
                IEnumerable<AlertDetail> alertDetails = _alertDetailRepository.GetAllList().Where(e => !e.IsDeleted && e.AlertID == alertId);
                formFields = _formFieldRepository.GetAllList().Where(e => alertDetails.Select(t => t.FormFieldID).Contains(e.Id)).OrderBy(e => e.OrderID).ToList();
            }
            else if (alert.GenerateTypeID == (long)AlertGeneratedType.Saved_Report && alert.ReportID.HasValue)
            {
                Report.Report report = _reportRepository.FirstOrDefault(r => r.Id == alert.ReportID);
                if (report != null)
                {
                    query = report.QueryStatement;
                    IEnumerable<ReportDetail> reportDetails = _reportDetailRepository.GetAllList().Where(e => !e.IsDeleted && e.ReportID == report.Id);
                    formFields = _formFieldRepository.GetAllList().Where(e => reportDetails.Select(t => t.FormFieldID).Contains(e.Id)).OrderBy(e => e.OrderID).ToList();
                }
            }

            #endregion



            #region 5) Get the receivers and send the email

            var recievers = Enumerable.Empty<object>()
                             .Select(r => new { EmailAddress = "", Id = 0L });

            if (alert.GenerateTypeID == (long)AlertGeneratedType.Saved_Report)
            {
                if (alert.IsSendToAll)
                {
                    recievers = _userRepository.GetAllList()
                        .Where(e => !e.IsDeleted && e.IsActive && e.UserType == (int)UserType.FendleyStaff)
                        .Select(e => new { e.EmailAddress, e.Id });
                }
                else
                {
                    List<long> alertRecievers = _alertRecieverRepository.GetAllList().Where(e => !e.IsDeleted && e.AlertID == alert.Id).Select(e => e.UserID).ToList();
                    recievers = _userRepository.GetAllList()
                        .Where(e => !e.IsDeleted && e.IsActive && alertRecievers.Contains(e.Id))
                        .Select(e => new { e.EmailAddress, e.Id });
                }
            }
            else if (alert.GenerateTypeID == (long)AlertGeneratedType.Query_Builder)
            {
                if (alert.IsSendToAll)
                {
                    recievers = _userRepository.GetAllList()
                            .Where(e => !e.IsDeleted && e.IsActive && e.UserType == alert.AlertToID)
                            .Select(e => new { e.EmailAddress, e.Id });
                }
                else {

                    List<long> alertRecievers = _alertRecieverRepository.GetAllList().Where(e => !e.IsDeleted && e.AlertID == alert.Id).Select(e => e.UserID).ToList();
                    recievers = _userRepository.GetAllList()
                        .Where(e => !e.IsDeleted && e.IsActive && alertRecievers.Contains(e.Id))
                        .Select(e => new { e.EmailAddress, e.Id });

                }
            }

            EmailSettingsEditDto emailSettings = await GetEmailSettingsAsync();
            Attachment emailAttachment = null;
            //emailSettings.SmtpUseDefaultCredentials 
            SmtpClient mailServer = new SmtpClient(emailSettings.SmtpHost, emailSettings.SmtpPort)
            {
                EnableSsl = emailSettings.SmtpEnableSsl,
                UseDefaultCredentials = emailSettings.SmtpUseDefaultCredentials,
                Credentials = new NetworkCredential(emailSettings.SmtpUserName, emailSettings.SmtpPassword)
            };
            try
            {
                foreach (var reciever in recievers)
                {

                    #region prepare attachment

                    #region 4) Get form fields columns and data, then export the file

                    List<string> columns = new List<string>();
                    DataTable output = new DataTable();
                    DataRow dataRow = null;
                    string columnName = string.Empty;
                    List<long> allowedFields = new List<long>();
                    // Filter columns as per permission if not super admin or impersonating
                    if (!isSuperAdmin(reciever.Id))
                    {
                        allowedFields = getFormFieldsWithPermission(reciever.Id);
                    }

                    foreach (FormField field in formFields)
                    {
                        // do not add alert columns
                        if (allowedFields.Any()) if (!allowedFields.Contains(field.Id)) continue;

                        columnName = field.DisplayName == string.Empty ? $"[{field.FormMasterField.FieldName}]" : field.DisplayName;
                        if (!output.Columns.Contains(columnName))
                        {
                            output.Columns.Add(columnName);
                        }
                        else
                        {
                            int sameColumnCtr = 1;
                            columnName += $"{sameColumnCtr}";
                            while (output.Columns.Contains(columnName))
                            {
                                columnName += $"{++sameColumnCtr}";
                            }
                            output.Columns.Add(columnName);
                        }

                        columns.Add(columnName);
                    }

                    if (!string.IsNullOrEmpty(query))
                    {
                        // Update query to filter result by user id permission
                        query = await ReplaceQueryToken(query, reciever.Id);
                        List<GetAlertResultDto> alertResultDtos = ExecuteAlertQuery(query);
                        foreach (long userID in alertResultDtos.Select(e => e.UserID).Distinct())
                        {
                            dataRow = output.NewRow();
                            List<string> inputDataFields = new List<string>();
                            foreach (var item in alertResultDtos.Where(e => e.UserID == userID))
                            {
                                dataRow[item.Field] = item.Value;
                                inputDataFields.Add(item.Field);
                            }

                            if (inputDataFields.Count < output.Columns.Count)
                            {
                                foreach (var column in output.Columns.Cast<DataColumn>())
                                {
                                    if (inputDataFields.IndexOf(column.ColumnName) == -1)
                                    {
                                        dataRow[column.ColumnName] = string.Empty;
                                    }
                                }
                            }
                            output.Rows.Add(dataRow);
                            dataRow = null;
                        }
                    }

                    FileDto file = _alertsExcelExporter.GenerateAlertExportToFile(output);
                    byte[] alertFile = _tempFileCacheManager.GetFile(file.FileToken);
                    emailAttachment = new Attachment(new MemoryStream(alertFile), $"{alert.Name}.xlsx", MediaTypeNames.Application.Octet);
                    #endregion

                    #endregion

                    MailMessage message = new MailMessage(
                        emailSettings.DefaultFromAddress,
                        reciever.EmailAddress,
                        alert.EmailSubject,
                        alert.EmailBody)
                    {
                        IsBodyHtml = true
                    };
                    // Add the file attachment to this email message.
                    message.Attachments.Add(emailAttachment);
                    //Send the message.
                    await mailServer.SendMailAsync(message);

                    alert.IsInProgress = false;
                    alert.LastEndDateTime = DateTime.UtcNow;
                    _alertRepository.Update(alert);
                }

            }
            finally
            {
                emailAttachment?.Dispose();
                mailServer?.Dispose();
            }

            #endregion
        }

        [AbpAllowAnonymous]
        public async Task<string> GenerateDashboardAlerts(long alert)
        {
            string output;
            DataTable result = new DataTable();
            DataRow dataRow = null;
            string query = string.Empty;

            Alert objAlert = await _alertRepository.GetAsync(alert);
            objAlert.IsInProgress = true;
            objAlert.LastStartDateTime = DateTime.Now;
            _alertRepository.Update(objAlert);

            List<FormField> formFields = null;
            if (objAlert.GenerateTypeID == (long)AlertGeneratedType.Query_Builder)
            {
                query = objAlert.QueryStatement;
                IEnumerable<AlertDetail> alertDetails = _alertDetailRepository.GetAllList().Where(e => !e.IsDeleted && e.AlertID == alert);
                using (_unitOfWorkManager.Current.DisableFilter(AbpDataFilters.SoftDelete))
                {
                    formFields = _formFieldRepository.GetAll().Where(e => alertDetails.Select(t => t.FormFieldID).Contains(e.Id)).OrderBy(e => e.OrderID).ToList();
                }
            }
            else if (objAlert.GenerateTypeID == (long)AlertGeneratedType.Saved_Report)
            {
                Report.Report report = await _reportRepository.GetAsync((long)objAlert.ReportID);
                query = report.QueryStatement;
                IEnumerable<ReportDetail> reportDetails = _reportDetailRepository.GetAllList().Where(e => !e.IsDeleted && e.ReportID == report.Id);
                using (_unitOfWorkManager.Current.DisableFilter(AbpDataFilters.SoftDelete))
                {
                    formFields = _formFieldRepository.GetAllList().Where(e => reportDetails.Select(t => t.FormFieldID).Contains(e.Id)).OrderBy(e => e.OrderID).ToList();
                }
            }

            List<string> columns = new List<string>();
            string columnName = string.Empty;
            List<long> allowedFields = new List<long>();
            // Filter columns as per permission if not super admin or impersonating
            if (!isSuperAdmin(AbpSession.UserId ?? 0) || _impersonator.IsLoginAsUser())
            {
                allowedFields = getFormFieldsWithPermission(AbpSession.UserId ?? 0);
            }

            foreach (FormField field in formFields)
            {
                // do not add alert columns
                if (allowedFields.Any()) if (!allowedFields.Contains(field.Id)) continue;

                columnName = field.DisplayName == string.Empty ? $"[{field.FormMasterField.FieldName}]" : field.DisplayName;
                if (!result.Columns.Contains(columnName))
                {
                    result.Columns.Add(columnName);
                }
                else
                {
                    int sameColumnCtr = 1;
                    columnName += $"{sameColumnCtr}";
                    while (result.Columns.Contains(columnName))
                    {
                        columnName += $"{++sameColumnCtr}";
                    }
                    result.Columns.Add(columnName);
                }

                columns.Add(columnName);
            }

            if (!string.IsNullOrWhiteSpace(query))
            {
                // Update query to filter result by user id permission
                query = await ReplaceQueryToken(query, AbpSession.UserId);
                List<GetAlertResultDto> alertResultDtos = ExecuteAlertQuery(query);
                foreach (long userId in alertResultDtos.Select(e => e.UserID).Distinct())
                {
                    dataRow = result.NewRow();
                    List<string> inputDataFields = new List<string>();
                    foreach (var item in alertResultDtos.Where(e => e.UserID == userId))
                    {
                        dataRow[item.Field] = item.Value;
                        inputDataFields.Add(item.Field);
                    }

                    if (inputDataFields.Count < result.Columns.Count)
                    {
                        foreach (var column in result.Columns.Cast<DataColumn>())
                        {
                            if (inputDataFields.IndexOf(column.ColumnName) == -1)
                            {
                                dataRow[column.ColumnName] = string.Empty;
                            }
                        }
                    }

                    result.Rows.Add(dataRow);
                    dataRow = null;
                }
            }

            output = Guid.NewGuid().ToString("N");
            _tempObjects.UpdateTemp(columns, _session_GeneratedAlertDetails + output);
            _tempObjects.UpdateTemp(result, _session_GeneratedAlert + output);

            return output;
        }

        public async Task<DataTable> GetDashboardGeneratedAlert(string input)
        {
            List<string> columns = await _tempObjects.GetTemp<List<string>>(_session_GeneratedAlertDetails + input);
            DataTable output = await _tempObjects.GetTemp<DataTable>(_session_GeneratedAlert + input);
            output = output ?? new DataTable();

            if (output.Rows.Count == 0)
            {
                foreach (string column in columns)
                {
                    output.Columns.Add(column);
                }
            }

            return output;
        }

        [AbpAllowAnonymous]
        public async Task<DataTable> TestQueryCall()
        {
            string query = "select * from FSPages";
            DataTable forreturn = new DataTable();
            using (SqlConnection con = new SqlConnection(_appConfiguration["ConnectionStrings:Default"]))
            {
                con.Open();
                using (SqlCommand com = new SqlCommand(query, con))
                {
                    using (var adapter = new SqlDataAdapter(com))
                    {
                        adapter.Fill(forreturn);
                        await Task.CompletedTask;
                    }
                }
            }
            return forreturn;
        }

        #region Private Methods
        private StringBuilder BuildQuery(SaveAlertDto input)
        {
            long pageID = input.PageID;
            StringBuilder query = new StringBuilder();
            StringBuilder select = new StringBuilder();
            select.AppendLine("SELECT");
            select.AppendLine("f.[Name] AS [Form]");
            select.AppendLine(",CASE WHEN ff.DisplayName = '' THEN fmf.FieldName WHEN ff.DisplayName IS NULL THEN fmf.FieldName ELSE ff.DisplayName END AS [Field]");
            //select.AppendLine(",ffd.[Value]");
            select.AppendLine(@",CASE 
                                    WHEN fmf.[LookupId] > 0 THEN fmlv.[Value]
                                    ELSE ffd.[Value]
                                END as [Value]"
                              );
            select.AppendLine(",ffd.UserID");
            select.AppendLine(",ffd.GroupInputId");

            StringBuilder from = new StringBuilder();
            from.AppendLine("FROM");
            from.AppendLine("FSFormFieldDatas ffd");
            from.AppendLine("LEFT JOIN");
            from.AppendLine("FSFormFields ff ON ffd.FormFieldID = ff.Id");
            from.AppendLine("LEFT JOIN");
            from.AppendLine("FSForms f ON ff.FormID = f.Id");
            from.AppendLine("LEFT JOIN");
            from.AppendLine($"FSPageForms pf ON f.Id = pf.FormID AND pf.PageID = {pageID}");
            from.AppendLine("LEFT JOIN");
            from.AppendLine("FSFormMasterFields fmf ON ff.FormMasterFieldID = fmf.Id");
            from.AppendLine("LEFT JOIN");
            from.AppendLine("FSFormMasterLookups fml ON fmf.LookupID = fml.Id");
            from.AppendLine("LEFT JOIN");
            from.AppendLine("FSFormMasterLookupValues fmlv on fml.id = fmlv.FormMasterLookupId");
            from.AppendLine("LEFT JOIN");
            from.AppendLine("FSPermissionDataAccesses fpda on ffd.FormFieldID = fpda.FormFieldID");
            from.AppendLine("LEFT JOIN");
            from.AppendLine("AbpUsers abpu on ffd.UserID = abpu.Id");

            
            from.AppendLine("LEFT JOIN");
            from.AppendLine("AbpUserOrganizationUnits abpou on ffd.UserID = abpou.UserId");

            StringBuilder where = new StringBuilder();

            // use IN clause in pageId as alert is now allowing multiple pages selecttion.
            string pageIDs = "0";
            if (input.AlertDetails.Count > 0) {
                pageIDs = string.Join(",", input.AlertDetails.Select(x => x.PageID));
            }

            where.AppendLine($"WHERE ffd.PageID in ({pageIDs})");
            where.AppendLine("AND");
            where.AppendLine($"ffd.FormFieldID IN (0");
            foreach (var item in input.AlertDetails)
            {
                where.AppendLine($",{item.FormFieldID}");
            }
            where.AppendLine($")");

            if (!input.HasNoCondition)
            {
                where.AppendLine("AND");
                where.AppendLine("(");
                int ctr = 0;
                foreach (var item in input.AlertQuery.QueryConditionsFields)
                {
                    if (ctr != 0)
                    {
                        where.AppendLine($"{item.Operator}");
                    }
                    where.AppendLine("(");
                    where.AppendLine($"ffd.FormFieldID = {item.FieldID}");
                    where.AppendLine("AND");
                    where.AppendLine("CASE ISNULL(fmf.LookupID, 1)");
                    where.AppendLine("WHEN 1 THEN [Value]");
                    where.AppendLine("ELSE (SELECT fmlv.[Value] FROM FSFormMasterLookupValues fmlv WHERE fmlv.FormMasterLookupId = fmf.LookupID AND fmlv.[Key] = ffd.[Value])");
                    where.AppendLine($"END {GetCondition(item.Condition)} '{item.Value}'");
                    where.AppendLine(")");

                    ctr++;
                }
                where.AppendLine(")");
            }

            // Make sure to replace this with correct query when doing manual, generate and background worker        
            where.AppendLine("AND 123=123");

            // Make sure to replace this with correct query when doing manual, generate and background worker        
            where.AppendLine("AND 345=345");

            // Add filter to show non deleted users
            where.AppendLine("AND");
            where.AppendLine("abpu.IsDeleted = 0");

            // Add filter for form master lookups values
            where.AppendLine(@"AND ffd.[Value]  =  
                                CASE
                                    WHEN fmf.[LookupId] is not null THEN fmlv.[Key]
                                    ELSE ffd.[Value]
                                END"
                            );

            query.AppendLine(select.ToString());
            query.AppendLine(from.ToString());
            query.AppendLine(where.ToString());
            query.AppendLine("ORDER BY");
            query.AppendLine("ffd.GroupInputId ASC, pf.OrderID ASC, ff.OrderID ASC");

            return query;
        }

        private string GetCondition(string condition)
        {
            string result = string.Empty;

            switch (condition.ToUpper())
            {
                case "EQUAL":
                    result = "=";
                    break;
                case "NOT EQUAL":
                    result = "<>";
                    break;
                case ">":
                    result = "<";
                    break;
                case ">=":
                    result = ">=";
                    break;
                case "<":
                    result = "<";
                    break;
                case "<=":
                    result = "<=";
                    break;
                default:
                    result = "=";
                    break;
            }

            return result;
        }

        private async Task<EmailSettingsEditDto> GetEmailSettingsAsync()
        {
            var smtpPassword = await SettingManager.GetSettingValueAsync(EmailSettingNames.Smtp.Password);

            return new EmailSettingsEditDto
            {
                DefaultFromAddress = await SettingManager.GetSettingValueAsync(EmailSettingNames.DefaultFromAddress),
                DefaultFromDisplayName = await SettingManager.GetSettingValueAsync(EmailSettingNames.DefaultFromDisplayName),
                SmtpHost = await SettingManager.GetSettingValueAsync(EmailSettingNames.Smtp.Host),
                SmtpPort = int.Parse(await SettingManager.GetSettingValueAsync(EmailSettingNames.Smtp.Port)),
                SmtpUserName = await SettingManager.GetSettingValueAsync(EmailSettingNames.Smtp.UserName),
                SmtpPassword = SimpleStringCipher.Instance.Decrypt(smtpPassword),
                SmtpDomain = await SettingManager.GetSettingValueAsync(EmailSettingNames.Smtp.Domain),
                SmtpEnableSsl = bool.Parse(await SettingManager.GetSettingValueAsync(EmailSettingNames.Smtp.EnableSsl)),
                SmtpUseDefaultCredentials = bool.Parse(await SettingManager.GetSettingValueAsync(EmailSettingNames.Smtp.UseDefaultCredentials))
            };
        }

        private List<GetAlertResultDto> ExecuteAlertQuery(string query)
        {
            var alertResultDtos = new List<GetAlertResultDto>();
            using (SqlConnection conn = new SqlConnection(_appConfiguration["ConnectionStrings:Default"]))
            {
                using (SqlCommand cmd = new SqlCommand()
                {
                    CommandText = query,
                    CommandType = CommandType.Text,
                    Connection = conn
                })
                {
                    cmd.Connection.Open();
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var alertResultDto = new GetAlertResultDto
                            {
                                Form = "" + reader[0],
                                Field = "" + reader[1],
                                Value = "" + reader[2],
                                UserID = long.Parse("" + reader[3]),
                                GroupInputId = long.Parse("" + reader[4])
                            };
                            alertResultDtos.Add(alertResultDto);
                        }
                    }
                }
            }
            return alertResultDtos;
        }

        private async Task<List<UserListDto>> GetUsersByOrgGroup()
        {
            // Query Users
            var query = UserManager.Users;
            // End of Query Users

            long currentUserId = AbpSession.GetUserId();

            if (currentUserId != 2)
            {
                //to protect super admin
                query = query.Where(e => (e.Id != 2 || currentUserId == 2));

                // Get Current User Final Org Unit
                Authorization.Users.User currentUser = await UserManager.FindByIdAsync(currentUserId.ToString());
                List<long> currentUserOrganizationUnits = new List<long>();
                currentUserOrganizationUnits.AddRange((await UserManager.GetOrganizationUnitsAsync(currentUser).ConfigureAwait(false))?.Select(e => e.Id)?.ToList());
                currentUserOrganizationUnits = currentUserOrganizationUnits ?? new List<long>();

                List<long> currentUserOrgGroupUnits = new List<long>();
                foreach (long currentUserOrganizationUnit in currentUserOrganizationUnits)
                {
                    if (CheckIfLastChildOfOrgUnitMappedToUser(currentUserOrganizationUnit))
                    {
                        currentUserOrgGroupUnits.Add(currentUserOrganizationUnit);
                    }
                    else
                    {
                        if (!CheckIfChildrenHasMappedToUser(currentUserOrganizationUnits, currentUserOrganizationUnit))
                        {
                            currentUserOrgGroupUnits.Add(currentUserOrganizationUnit);
                        }
                    }
                }
                // End of Get Current User Final Org Unit

                Authorization.Users.User userResult = null;
                List<long> userOrganizationUnits = null;
                List<long> userOrgGroupUnits = null;
                List<long> usersInGroup = new List<long>();
                foreach (var item in query)
                {
                    userResult = await UserManager.FindByIdAsync(item.Id.ToString());

                    userOrganizationUnits = new List<long>();
                    userOrganizationUnits.AddRange((await UserManager.GetOrganizationUnitsAsync(userResult).ConfigureAwait(false))?.Select(e => e.Id)?.ToList());
                    userOrganizationUnits = userOrganizationUnits ?? new List<long>();

                    userOrgGroupUnits = new List<long>();
                    foreach (long userOrganizationUnit in userOrganizationUnits)
                    {
                        if (CheckIfLastChildOfOrgUnitMappedToUser(userOrganizationUnit))
                        {
                            userOrgGroupUnits.Add(userOrganizationUnit);
                        }
                        else
                        {
                            if (!CheckIfChildrenHasMappedToUser(userOrganizationUnits, userOrganizationUnit))
                            {
                                userOrgGroupUnits.Add(userOrganizationUnit);
                            }
                        }
                    }

                    if (userOrgGroupUnits.Where(e => currentUserOrgGroupUnits.Contains(e)).Any())
                    {
                        usersInGroup.Add(userResult.Id);
                    }

                    userResult = null;
                    userOrganizationUnits = null;
                    userOrgGroupUnits = null;
                }
                query = query.Where(e => usersInGroup.Contains(e.Id));
            }

            var userCount = await query.CountAsync();
            var users = await query
                .ToListAsync();

            var userListDtos = ObjectMapper.Map<List<UserListDto>>(users);
            return userListDtos;
        }

        private bool CheckIfChildrenHasMappedToUser(List<long> currentUserOrganizationUnits, long orgId)
        {
            bool result = false;

            List<long> children = _organizationUnitRepository.GetAll().Where(e => e.ParentId == orgId).Select(e => e.Id).ToList();
            List<long> childAndChildChildren = new List<long>();
            foreach (long child in children)
            {
                childAndChildChildren.Add(child);
                childAndChildChildren.AddRange(GetOrgChildren(child));
            }

            childAndChildChildren = childAndChildChildren.Where(e => currentUserOrganizationUnits.Contains(e)).ToList();

            if (childAndChildChildren.Count > 0)
                result = true;

            return result;
        }

        private List<long> GetOrgChildren(long parent)
        {
            List<long> result = new List<long>();

            List<long> children = _organizationUnitRepository.GetAll().Where(e => e.ParentId == parent).Select(e => e.Id).ToList();
            foreach (long child in children)
            {
                result.Add(child);
                result.AddRange(GetOrgChildren(child));
            }

            return result;
        }

        private bool CheckIfLastChildOfOrgUnitMappedToUser(long orgId)
        {
            bool result = false;

            List<long> children = _organizationUnitRepository.GetAll().Where(e => e.ParentId == orgId).Select(e => e.Id).ToList();
            children = children ?? new List<long>();

            if (children.Count == 0)
                result = true;

            return result;
        }

        private List<long> GetParent(long child)
        {
            List<long> result = new List<long>();

            long? parent = _organizationUnitRepository.Get(child).ParentId;
            if (parent.HasValue)
            {
                result.Add((long)parent);
                result.AddRange(GetParent((long)parent));
            }

            return result;
        }

        private List<AlertDashboarDto> GetExpiryAlerts(AlertType alertType)
        {

            var data = new List<AlertDashboarDto>();
            var alertsWhereLoginUserIsReceiver = this.GetAlertIdsWhereUserLoggedIsTheReceiver();

            // get alert type 
            var alerts = _alertRepository
                    .GetAll()
                    .Where(e => !e.IsDeleted
                        && e.Status
                        && e.SendinDashboard
                        && e.AlertTypeID == (long)alertType
                        && e.StartDate.Date <= DateTime.Now.Date
                        && (e.IsSendToAll || alertsWhereLoginUserIsReceiver.Contains(e.Id)))
                    .ToList();

            foreach (var alert in alerts)
            {
                var dto = new AlertDashboarDto();
                dto.AlertGroup = "EXPIRY";
                dto.Date = alert.EndDate ?? DateTime.Now;
                dto.AlertId = alert.Id;
                dto.AlertText = alert.DashboardTitle;
                dto.AlertType = Enum.GetName(typeof(AlertType), alertType);
                data.Add(dto);
                dto = null;
            }

            return data;

        }

        private List<long> GetAlertIdsWhereUserLoggedIsTheReceiver()
        {
           var alerts = _alertRecieverRepository.GetAll()
                .Where(r => r.UserID == AbpSession.UserId && !r.IsDeleted)
                .Select(r => r.AlertID).ToList();

            return alerts;
        }

        private async Task<string> ReplaceQueryToken(string query, long? userId)
        {

            // Replace Today + 30 days token
            if (query.IndexOf("'Today + 30 days'") > 0)
            {
                query = query.Replace("'Today + 30 days'", "CONVERT(datetime, DATEADD(DAY, 30, GETDATE()) ,103)");
            }

            // Replace Today + 60 days token
            if (query.IndexOf("'Today + 60 days'") > 0)
            {
                query = query.Replace("'Today + 60 days'", "CONVERT(datetime, DATEADD(DAY, 60, GETDATE()) ,103)");

            }

            // Replace Today + 90 days token
            if (query.IndexOf("'Today + 90 days'") > 0)
            {
                query = query.Replace("'Today + 90 days'", "CONVERT(datetime, DATEADD(DAY, 90, GETDATE()) ,103)");

            }

            await Task.CompletedTask;

            string roleIds = "0";
            string orgUnitIds = "0";

            var userRoles = _userRoleRepository.GetAll()
                .Where(x => x.UserId == userId);

            if (userRoles.Any()) roleIds = string.Join(",", userRoles.Select(a => a.RoleId));

            var userOgrUnitIds = _userOrganizationUnit.GetAll()
                .Where(x => x.UserId == userId && !x.IsDeleted);

            if (userOgrUnitIds.Any()) orgUnitIds = string.Join(",", userOgrUnitIds.Select(a => a.OrganizationUnitId));


            if (query.IndexOf("AND 123=123") > 0)
            {

                // If current user is not super admin || impersonating, add permission to where clause             
                if (!isSuperAdmin(userId) || _impersonator.IsLoginAsUser())
                {
                    query = query.Replace("AND 123=123", "AND fpda.RecordReport = 1 AND fpda.RoleID in (" + roleIds + ")");
                }

            }

            if (query.IndexOf("AND 345=345") > 0)
            {

                // If current user is not super admin || impersonating, filter to show users that belongs to the same client groups          
                if (!isSuperAdmin(userId) || _impersonator.IsLoginAsUser())
                {
                    
                    
                    if (!isResourceUser(userId))
                    {
                        query = query.Replace("AND 345=345", "AND abpou.OrganizationUnitId IN (" + orgUnitIds + ") AND abpou.IsDeleted = 0");
                    }
                    else
                    {
                        // Add userId filter if receiver is resource user so that resource user is only able to view own data
                        query = query.Replace("AND 345=345", "AND abpou.OrganizationUnitId IN (" + orgUnitIds + ") AND abpou.IsDeleted = 0 AND abpou.UserId =" + userId);
                        
                    }
                }

                

            }

            return query;

        }

        private bool isSuperAdmin(long? userId)
        {

            var superAmdminRole = _roleManager.Roles.Where(x => x.DisplayName.Trim().ToLower() == "super admin").FirstOrDefault();

            var userRoles = _userRoleRepository.GetAll()
                .Where(x => x.UserId == userId);

            return userRoles.Any(x => x.RoleId == superAmdminRole.Id);
        }

        private bool isResourceUser(long? userId)
        {

            var resourceUserRole = _roleManager.Roles.Where(x => x.DisplayName.Trim().ToLower() == "resource user").FirstOrDefault();

            var userRoles = _userRoleRepository.GetAll()
                .Where(x => x.UserId == userId);

            return userRoles.Any(x => x.RoleId == resourceUserRole.Id);
        }

        private List<long> getFormFieldsWithPermission(long? userId)
        {
            var superAmdminRole = _roleManager.Roles.Where(x => x.DisplayName.Trim().ToLower() == "super admin").FirstOrDefault();

            var userRoles = _userRoleRepository.GetAll()
                .Where(x => x.UserId == userId)
                .Select(x => (long)x.RoleId).ToList();

            var fieldsWithAcess = _permissionDataAccessRepository.GetAll()
                .Where(f => f.RecordReport && userRoles.Contains(f.RoleID))
                .Select(f => f.FormFieldID).ToList();

            return fieldsWithAcess;
        }

        #endregion Private Methods
    }
}
