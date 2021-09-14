using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using Tessa.Cards;
using Tessa.Extensions.Server.ExternalFiles.Resolvers;
using Tessa.Extensions.Shared.ExternalFiles;
using Tessa.Extensions.Shared.Helpers;
using Tessa.Platform.Data;
using Tessa.Platform.Runtime;
using Tessa.Platform.Storage;
using Tessa.Platform.Validation;



namespace Tessa.Extensions.Server.ExternalFiles.Executors
{
    public sealed class KsSendFileExecutor : SendFileExecutorBase
    {

        #region Fields

        private readonly IExternalFilesResolver resolver;
        private readonly ICardStreamServerRepository cardStreamRepository;
        private readonly Logger logger = LogManager.GetCurrentClassLogger();
        //        private const string GetParamsSql = @"
        //SELECT TOP 1
        //    dt.Code -- Код типа документа
        //    ,pr.ClockNumber -- Табельный номер сотрудника 
        //    ,dep.Code  -- Код департамента сотрудника
        //    ,prod.Code  -- Код продукта
        //    ,modul.DossierType -- Вид Досье
        //    , dt.SubNumber --Подтип документа
        //FROM Vtb24Departments dep WITH(NOLOCK), PersonalRoles pr WITH(NOLOCK), Vtb24DictProducts prod WITH(NOLOCK), Vtb24DictDocumentTypes dt WITH(NOLOCK)
        //JOIN Vtb24DictModules modul WITH(NOLOCK) on modul.ID = dt.ModuleID
        //WHERE pr.ID = @UserID AND dep.ID = pr.DepartmentID AND dt.ID = @DocTypeID AND prod.ID = @ProductID";

        private const string GetParamsSql = @"
SELECT TOP 1
    dt.Code -- Код типа документа
    ,pr.ClockNumber -- Табельный номер сотрудника 
    ,dep.Code  -- Код департамента сотрудника
    ,prod.Code  -- Код продукта
    , dt.SubNumber --Подтип документа
FROM Vtb24Departments dep WITH(NOLOCK), PersonalRoles pr WITH(NOLOCK), Vtb24DictProducts prod WITH(NOLOCK), Vtb24DictDocumentTypes dt WITH(NOLOCK)
WHERE pr.ID = @UserID AND dep.ID = pr.DepartmentID AND dt.ID = @DocTypeID AND prod.ID = @ProductID";


        private const string GetDocTypeIdSql = @"
SELECT DISTINCT
    DFF.FileId,
  DT.ID,
(select ';' + LOWER(FileExtName) 
  from  Vtb24DictDocTypeExtensions with(nolock) 
  where ForSUA = 1 and ID = DT.ID FOR XML PATH('')) + ';',
DT.ModuleCode
FROM 
(
  SELECT  DF.FileId,DF.FileTypeID FROM Vtb24KsCreditDossierTaskFileInfo DF WITH(NOLOCK) 
  WHERE DF.ID=@cardId 
  AND DF.Active = 1
  AND DF.FileVersion = 
  (  
    SELECT MAX(f.FileVersion) from Vtb24KsCreditDossierTaskFileInfo f WITH(NOLOCK)  
    where f.FileId=df.FileId and f.FileTypeID=df.FileTypeID
  )
) DFF
INNER JOIN Vtb24DictDocumentTypes DT WITH(NOLOCK) ON DFF.FileTypeID=DT.ID
INNER JOIN 
(
  SELECT F.ID,COUNT(F.MemberRowID) as CountMember  FROM Vtb24KsFileMember F WITH(NOLOCK)
  INNER JOIN Vtb24KsMembers m WITH(NOLOCK) ON f.MemberRowID=m.RowID
  WHERE m.Active=1 GROUP BY F.ID
)  FM ON FM.ID=DFF.FileID
";
        // NotToBeMirrored на самом деле по смыслу противоположен, т.е. true означает, что отображаться должен
        private const string GetContractNumberDatesSql = @"
SELECT 
	pc.ProductDossierDate, 
	pc.ProductDossierNumber,
    pc.NotToBeMirrored
FROM Vtb24KsTasks kt with(nolock)
JOIN Vtb24KsProductDossierCollection pc WITH(NOLOCK) ON kt.ID = pc.ID
WHERE kt.ID = @taskID
";
        private const string getFileSourceID = @"select fv.SourceID from FileVersions fv with(nolock) left join Files f with(nolock) on fv.RowID = f.VersionRowID
            where f.RowID = @RowID";

        private const string getDocTypeModuleCode = @"select coalesce(ddt.ModuleCode, '0')
            from Vtb24DictDocumentTypes ddt with(nolock) left join Vtb24KsCreditDossierTaskFileInfo tfi with(nolock) on ddt.ID = tfi.FileTypeID
            where tfi.FileId = @FileID";

        private const string getUserClockNumber = @"
        SELECT TOP 1
            pr.ClockNumber -- Табельный номер сотрудника 
            , pr.[Name] -- ФИО сотрудника
        FROM PersonalRoles pr WITH(NOLOCK)
        WHERE pr.ID = @UserID";

        private const string getCardModule = @"select top 1 m.Code, m.TypeDossierName
from Vtb24DictOperationTypes  dot with(nolock) join Vtb24DictModules m with(nolock) on dot.ModuleID = m.ID
where dot.ID = @ID";

        private const string getFileSectionID = @"select ddt.SectionID from Vtb24DictDocumentTypes as ddt with(nolock) join Vtb24KsCreditDossierTaskFileInfo as tfi with(nolock) 
on tfi.FileTypeID = ddt.ID where tfi.FileId = @RowID";


        private const string switchFileVersionSql = @"
DECLARE @initFileId uniqueidentifier;
DECLARE @initFileName varchar(255);
DECLARE @initFileVersionId uniqueidentifier;
DECLARE @initFileVersion int;

SELECT @initFileId = f.RowID, @initFileName = f.Name, @initFileVersionId = f.VersionRowID, @initFileVersion = f.VersionNumber FROM Files f WITH(NOLOCK) 
WHERE f.ID = @cardId 
AND f.RowID <> @fileId 
AND (f.Name = @fileName OR (f.Name LIKE '%.tif%' AND (f.Name + '.pdf' = @fileName OR f.Name = @fileName + '.pdf')));

IF @initFileId IS NOT NULL
BEGIN
	UPDATE Vtb24KsCreditDossierTaskFileInfo 
	SET 
		FileId = @initFileId,
		FileName = @initFileName,
		FileVersionId = @initFileVersionId,
		FileVersion = @initFileVersion
	WHERE ID = @cardId AND FileId = @fileId 
END";

        #endregion

        #region Constructors

        public KsSendFileExecutor(
            IDbScope dbScope,
            ISession session,
            ICardStreamServerRepository cardStreamRepository,
            IExternalFilesResolver resolver
            ):base(dbScope, session)
        {
            this.cardStreamRepository = cardStreamRepository ?? throw new ArgumentNullException(nameof(cardStreamRepository));
            this.resolver = resolver ?? throw new ArgumentNullException(nameof(resolver));
        }

        #endregion

        #region ISendFilesExecutor

        public override void PostCard(Guid cardID, Dictionary<string, object> parameters = null, string executorName = null)
        {
            logger.Trace($"Карточка отправлена в очередь CardID: {cardID}");
            base.PostCard(cardID, parameters, executorName ?? ExternalFilesHelper.ExecutorNames.KsSendFileExecutor);
        }

        public override ProcessedFileInfo ProcessCard(Card card, Guid userID, string userName, Dictionary<string, object> parameters)
        {
            bool eDossier;
            List<object> completedFiles;
            Tuple<int?, string> moduleData;
            //int? moduleCode = null;
            if ((completedFiles = parameters.TryGet<List<object>>("CompletedFiles")) == null)
            {
                completedFiles = new List<object>();
                parameters["CompletedFiles"] = completedFiles;
                logger.Trace($"Карточка {card.ID}: Создана новая коллекция completedFiles");
            }
            var processedFileInfo = new ProcessedFileInfo();

            var cardSection = card.Sections.GetOrAdd(CreditSolutionHelper.Sections.Vtb24KsTasksSection);
            if (cardSection.Fields.ContainsKey("CreatedByService") && cardSection.Fields["CreatedByService"] as bool? == true && !parameters.ContainsKey("formEFR") )
            {
                logger.Trace($"Карточка {card.ID}: создана сервисом, файлы не будут обработаны");
                processedFileInfo.ValidationResultBuilder.Add(ValidationResult.Empty);
                return processedFileInfo;
            }
            if(cardSection.Fields.ContainsKey("OperationTypeID") && cardSection.Fields["OperationTypeID"]?.ToString().Trim().Length > 0)
            {
                moduleData = GetCardModule(Guid.Parse(cardSection.Fields["OperationTypeID"].ToString()));
            }
            else
            {
                processedFileInfo.ValidationResultBuilder.Add(ValidationResult.FromText(($"Отсутствует тип операции в карточке {card.ID}.")));
                logger.Error($"Отсутствует тип операции в карточке {card.ID}.");
                return processedFileInfo;
            }
            if (cardSection.Fields.ContainsKey("EDossier"))
            {
                eDossier = cardSection.Fields["EDossier"] is bool && (bool)cardSection.Fields["EDossier"] == true;
            }
            else
            {
                eDossier = false;
            }
            if (moduleData == null ||  moduleData.Item1 == null)
            {
                processedFileInfo.ValidationResultBuilder.Add(ValidationResult.FromText(($"Не удалось получить код модуля для карточки {card.ID}.")));
                logger.Error($"Не удалось получить код модуля для карточки {card.ID}.");
                return processedFileInfo;
            }
            if(moduleData.Item2 == string.Empty)
            {
                processedFileInfo.ValidationResultBuilder.Add(ValidationResult.FromText(($"Не удалось получить тип досье для карточки {card.ID}.")));
                logger.Error($"Не удалось получить тип досье для карточки {card.ID}.");
                return processedFileInfo;
            }
            int storeSourceID;
            var filesDoctypes = GetDocumentTypes(card);
            logger.Trace($"Карточка {card.ID}: Получены типы документов {filesDoctypes.Count}");
            bool successful;
            
            foreach (var file in card.Files)
            {
                successful = false;
                if (filesDoctypes.ContainsKey(file.RowID) && filesDoctypes[file.RowID].Item2.ToLowerInvariant().Contains($"{Path.GetExtension(file.Name).ToLowerInvariant()};"))
                {
                    file.CategoryID = filesDoctypes[file.RowID].Item1;
                }

                if (!file.CategoryID.HasValue)
                {
                    // Если у файла отсутствует категория, то файл не отправляем
                    logger.Trace($"Файл {file.RowID} без категории! Не отправлено.");
                    continue;
                }
                if (completedFiles.Contains(file.RowID))
                {
                    logger.Trace($"Карточка {card.ID}: Файл {file.RowID} уже обработан");
                    // Если файл уже был обработан, то повторно его не отправляем
                    continue;
                }
                storeSourceID = GetFileSourceID(file.RowID);
                if (storeSourceID == (int)ReusableFileSource.ClientDatabaseContent || storeSourceID == (int)ReusableFileSource.ProductDatabaseContent
                        || storeSourceID == (int)ReusableFileSource.CurrentDatabaseContent)
                {
                    //файлы опубликованы, не отправляем
                    completedFiles.Add(file.RowID);
                    continue;
                }
                if(filesDoctypes[file.RowID].Item3 == RkoSolutionHelper.Modules.RkoModuleCode)
                //if(GetFileCatID(file.RowID) == RkoSolutionHelper.RkoFileCategories.Client)
                //if(moduleData.Item1 == RkoSolutionHelper.Modules.RkoModuleCode)
                {
                    completedFiles.Add(file.RowID);
                    continue;
                }
                Func<Stream> getContentFunc;
                var response = this.cardStreamRepository.GetFileContent(
                    new CardGetFileContentRequest
                    {
                        CardID = card.ID,
                        FileID = file.RowID,
                        VersionRowID = file.VersionRowID,
                        FileName = file.Name
                    }, out getContentFunc);

                if (!response.ValidationResult.IsSuccessful())
                {
                    processedFileInfo.ValidationResultBuilder.Add(response.ValidationResult.Build());
                    logger.Error($"Карточка {card.ID}: Контент файла RowID: {file.RowID} получен с ошибкой {response.ValidationResult.Build()}");
                    continue;
                    return processedFileInfo;
                }

                var contractsDates = GetTaskContractNumbersAndDates(card.ID);

                using (var stream = getContentFunc())
                {
                    var data = new byte[stream.Length];
                    stream.Read(data, 0, (int)stream.Length);

                    SentFileInfo fileInfo;
                    if (contractsDates != null)
                    {
                        foreach (var cd in contractsDates)
                        {
                            fileInfo = this.resolver.SendFile(data, file.Name, this.PrepareSendParameters(card, file, moduleData.Item2, eDossier, cd.Key, cd.Value));
                            if (fileInfo != null && fileInfo.ValidationResult.IsSuccessful())
                            {
                                processedFileInfo.Add(fileInfo);
                                successful = true;
                                logger.Trace($"Карточка {card.ID}: Файл: {file.RowID}, Дата {cd.Value} Отправлен успешно");
                            } else if (fileInfo != null)
                            {
                                processedFileInfo.ValidationResultBuilder.Add(fileInfo.ValidationResult.Build());
                                logger.Trace($"Карточка {card.ID}: Файл: {file.RowID}, Дата {cd.Value} не был успешно отправлен {fileInfo.ValidationResult.Build()}");
                            }
                        }
                    }
                    else
                    {
                        fileInfo = this.resolver.SendFile(data, file.Name, this.PrepareSendParameters(card, file, moduleData.Item2, eDossier));
                        if (fileInfo.ValidationResult.IsSuccessful())
                        {
                            processedFileInfo.Add(fileInfo);
                            successful = true;
                            logger.Trace($"Карточка {card.ID}: Файл: {file.RowID},  Отправлен успешно");
                        }
                        else
                        {
                            processedFileInfo.ValidationResultBuilder.Add(fileInfo.ValidationResult.Build());
                            logger.Trace($"Карточка {card.ID}: Файл: {file.RowID} не был успешно отправлен {fileInfo.ValidationResult.Build()}");
                        }
                    }
                    if(successful)
                    {
                        completedFiles.Add(file.RowID);
                    }

                }

            }
            return processedFileInfo;
        }

        #endregion

        #region Private Methods

        private Dictionary<string, object> PrepareSendParameters(Card card, CardFile file, string dossierType, bool eDossier, string contractNumber = null, DateTime? contractDate = null)
        {
            Guid initiatorID;
            var result = new Dictionary<string, object>();
            var mainSection = card.Sections[CreditSolutionHelper.Sections.Vtb24KsTasksSection];

            result[ExternalFilesHelper.ParamNames.FileID] = file.RowID;
            result[ExternalFilesHelper.ParamNames.FileVersionID] = file.VersionRowID;
            result[ExternalFilesHelper.ParamNames.FileName] = file.Name;
            result[ExternalFilesHelper.ParamNames.ClientDVID] = mainSection.RawFields.TryGet<Guid?>("NaturalClientID")?? mainSection.RawFields.TryGet<Guid>("LegalClientID");
            result[ExternalFilesHelper.ParamNames.ContractDate] = contractDate ?? mainSection.RawFields.TryGet<DateTime?>("ContractDate") ?? DateTime.Now.Date;
            result[ExternalFilesHelper.ParamNames.ContractNum] = contractNumber ?? mainSection.RawFields.TryGet<string>("ContractNumber");
            result[ExternalFilesHelper.ParamNames.DocComment] = string.Empty;
            result[ExternalFilesHelper.ParamNames.DocDate] = file.LastVersion.Created;
            result[ExternalFilesHelper.ParamNames.DocNum] = file.LastVersion.Number.ToString();
            result[ExternalFilesHelper.ParamNames.TessaTaskID] = card.ID;
            result[ExternalFilesHelper.ParamNames.BarCode] = mainSection.RawFields.TryGet<string>("Barcode");
            result[ExternalFilesHelper.ParamNames.DossierVersion] = eDossier;
            result[ExternalFilesHelper.ParamNames.BranchID] = mainSection.RawFields.TryGet<string>("BranchCode");

            // Данные параметры заполняются с помощью получения информации из БД
            initiatorID = mainSection.RawFields.TryGet<Guid>(ExternalFilesHelper.ParamNames.InitiatorID, Guid.Empty);
            
            if(initiatorID == Guid.Empty)
            {
                logger.Error($"В секции карточки {card.ID} отсутсвует значение ID инициатора.");
                return result;
            }
            using (this.DbScope.Create())
            {
                var db = this.DbScope.Db;
                db.SetCommand(getUserClockNumber, db.Parameter("UserID", initiatorID));
                using (var reader = db.ExecuteReader())
                {
                    if(reader.Read())
                    {
                        result[ExternalFilesHelper.ParamNames.ManagerNum] = reader.GetValue<long>(0).ToString();
                        result[ExternalFilesHelper.ParamNames.Manager] = reader.GetValue<string>(1);
                    }
                }
                db.SetCommand(GetParamsSql,
                            db.Parameter("ProductID", mainSection.RawFields.TryGet<Guid>("ProductID")),
                            db.Parameter("DocTypeID", file.CategoryID),
                            db.Parameter("UserID", initiatorID)).LogCommand();
                using (var reader = db.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        result[ExternalFilesHelper.ParamNames.DocType] = reader.GetValue<string>(0);
                        result[ExternalFilesHelper.ParamNames.OfficeCode] = reader.GetValue<string>(2);
                        result[ExternalFilesHelper.ParamNames.ProductCode] = reader.GetValue<string>(3);
                        result[EAProductDossierExternalFilesResolver.ParamNames.DocSubType] = reader.GetValue<string>(4) ?? string.Empty;
                    }
                }
                result[ExternalFilesHelper.ParamNames.DossierType] = dossierType;
            }
            return result;
        }


        private Dictionary<string, DateTime?> GetTaskContractNumbersAndDates(Guid taskID)
        {
            var hasNewNumbers = false; //признак того, что номера договоров в новом формате (в виде коллекционной секции ProductDossierCollection)
            using (this.DbScope.Create())
            {
                var result = new Dictionary<string, DateTime?>();

                var db = this.DbScope.Db;
                db.SetCommand(GetContractNumberDatesSql,
                        db.Parameter("@taskID", taskID )).LogCommand();
                using (var reader = db.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        hasNewNumbers = true; //если есть хотя бы одна запись, то формат новый
                        var conNum = reader.GetValue<string>(1);
                        var conDate = reader.GetValue<DateTime?>(0);
                        var show = reader.GetValue<bool>(2);
                        if (!result.ContainsKey(conNum) && show) result.Add(conNum, conDate);    
                    }
                }
                return (hasNewNumbers) ? result : null; //таким образом, если вернется null, это будет означать, что номер контракта нужно искать по-старому
            }
        }

        /// <summary>
        /// Получить словарь с типами документов файлов (key - id файла, value - id типа документа)
        /// </summary>
        /// <param name="card">Карточка запроса</param>
        /// <returns></returns>
        private Dictionary<Guid?, Tuple<Guid?, string, int>> GetDocumentTypes(Card card)
        {
            var res = new Dictionary<Guid?, Tuple<Guid?, string, int>>();
            
            using (DbScope.Create())
            {
                var db = DbScope.Db;
                db.SetCommand(GetDocTypeIdSql, db.Parameter("cardId", card.ID)).LogCommand();
                using (var reader = db.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var fileRowId = reader.GetValue<Guid?>(0);
                        var docTypeId = reader.GetValue<Guid?>(1);
                        var availableExts = reader.GetValue(2);
                        var moduleCode = reader.GetValue(3);
                        res.Add(fileRowId, new Tuple<Guid?, string, int> (docTypeId, availableExts == null ? string.Empty : availableExts.ToString(), 
                            moduleCode == null || string.IsNullOrEmpty(moduleCode.ToString()) ? -1 : (int)moduleCode));
                    }
                }
                return res;
            }
        }

        private int GetFileSourceID(Guid RowID)
        {
            using (this.DbScope.CreateNew())
            {
                var db = this.DbScope.Db;
                var res = db.SetCommand(getFileSourceID, db.Parameter("RowID", RowID)).LogCommand().ExecuteScalar();
                if (res != null)
                {
                    return int.Parse(res.ToString());
                }
                else
                {
                    return -1;
                }
            }
        }

        private Tuple<int?, string> GetCardModule(Guid operType)
        {
            Tuple<int?, string> res;
            using (DbScope.CreateNew())
            {
                var db = DbScope.Db;
                db.SetCommand(getCardModule, db.Parameter("ID", operType));
                using (var reader = db.ExecuteReader())
                {
                    if(reader.Read())
                    {
                        res = new Tuple<int?, string> (reader.GetValue<int?>("Code"), reader.GetValue(1) == DBNull.Value ? string.Empty : reader.GetValue<string>("TypeDossierName"));
                        return res;
                    }
                }
            }
            return null;
        }

        private int GetDocTypeModuleCode(Guid FileID)
        {
            using (DbScope.CreateNew())
            {
                var db = DbScope.Db;
                var res = db.SetCommand(getDocTypeModuleCode, db.Parameter("FileID", FileID)).LogCommand().ExecuteScalar();
                if(res != null)
                {
                    return int.Parse(res.ToString());
                }
                else
                {
                    return -1;
                }
            }
        }

        private int GetFileCatID(Guid RowID)
        {
            using (this.DbScope.CreateNew())
            {
                var db = this.DbScope.Db;
                var res = db.SetCommand(getFileSectionID, db.Parameter("RowID", RowID)).LogCommand().ExecuteScalar();
                if (res != null && res != DBNull.Value)
                {
                    return RkoTasksFileHelper.SectionIDToCategoryID((int)res);
                }
                else
                {
                    return -1;
                }
            }
        }

        /// <summary>
        /// Поставить исходный файл (до штамповки или конвертации в pdf) основным для публикации в досье, либо вернуть обратно, если исходный уже был выставлен
        /// </summary>
        /// <param name="fileId"></param>
        /// <param name="fileName"></param>
        /// <param name="cardId"></param>
        private void SwitchFileVersion(Guid fileId, string fileName, Guid cardId)
        {
            using (this.DbScope.CreateNew())
            {
                var db = this.DbScope.Db;
                var res = db.SetCommand(switchFileVersionSql, 
                                        db.Parameter("@fileId", fileId),
                                        db.Parameter("@fileName", fileName),
                                        db.Parameter("@cardId", cardId)
                    ).LogCommand().ExecuteScalar();
                

            }
        }

        #endregion

    }
}
