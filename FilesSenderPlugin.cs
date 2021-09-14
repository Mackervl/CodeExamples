using Chronos.Contracts;
using Microsoft.Practices.Unity;
using NLog;
using System;
using System.Collections.Generic;
using Tessa.Cards;
using Tessa.Cards.ComponentModel;
using Tessa.Extensions.Server.ExternalFiles.Executors;
using Tessa.Extensions.Shared.ExternalFiles;
using Tessa.Extensions.Shared.Helpers;
using Tessa.Platform;
using Tessa.Platform.Data;
using Tessa.Platform.Validation;
using Tessa.Extensions.Chronos.Plugins.FilesSenderPlugin.Base;

namespace Tessa.Extensions.Chronos.Plugins.FilesSenderPlugin.SUA
{
    [Plugin(
        Name = "FilesSenderPlugin",
        Description = "Плагин производит отправку файлов во внешние системы",
        Version = 1,
        ConfigFile = ConfigFilePath)]
    public sealed class FilesSenderPlugin :
        BaseFilesSenderPlugin
    {
        #region SQLs
        private const string GetCardsForProcessSql = @"
DECLARE @startFullLoadTime AS TIME = '20:00:00';
DECLARE @endFullLoadTime AS TIME = '23:59:59';
DECLARE @currentUtcTime AS TIME = CAST(GETUTCDATE() AS TIME);

SELECT ID, CardID, UserID, UserName, ExecutorName, Parameters, ISNULL(Attempts,0) as Attempts FROM " + sourceTable + @" WITH(NOLOCK)
WHERE ProcessID = @ProcessID AND (ISNULL(Attempts,0) < 4 OR @currentUtcTime BETWEEN @startFullLoadTime AND @endFullLoadTime) 
ORDER BY LastError";

        private const string BlockCardsSql = @"
DECLARE @utcNow datetime = GETUTCDATE();
UPDATE " + sourceTable +
@" SET ProcessID = @ProcessID, InWork = @utcNow 
WHERE (ProcessID is Null OR datediff(minute, InWork, @utcNow) > 30) AND (LastError is Null OR datediff(minute, LastError, @utcNow) > 30)
";

        private const string DeleteCardAfterProcessingSql = @"DELETE FROM " + sourceTable + @" WHERE ID = @ID";

        private const string UpdateCardAfterProcessingSql = @"
UPDATE " + sourceTable +
@" SET ProcessID = NULL, InWork = NULL, LastErrorText = @ErrorText, LastError = GETUTCDATE(), Parameters = @Parameters, Attempts = ISNULL(Attempts,0) + 1
WHERE ID = @ID";
        #endregion

        #region Consts

        private const string ConfigFilePath = "configuration/FilesSenderPlugin.xml";
        private new const string sourceTable = ExternalFilesHelper.FilesForSendTable;
        #endregion

        #region Fields

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        #endregion

        #region Private Methods

        /// <summary>
        /// Метод для блокировки записей на обработку
        /// </summary>
        private void BlockFiles(Guid processID)
        {
            using (this.dbScope.Create())
            {
                var executor = this.dbScope.Executor;
                executor.ExecuteNonQuery(BlockCardsSql, executor.Parameter("ProcessID", processID));
            }
        }

        protected override void ProcessCard(CardForProcess cardInfo)
        {
            int updateRes = 0;
            var successful = false;
            logger.Trace("Начата обработка записи {0} для карточки {1} с обработчиком {2}", cardInfo.ID, cardInfo.CardID, cardInfo.ExecutorName);
            try
            {
                if (!this.fileExecutors.TryGetValue(cardInfo.ExecutorName, out ISendFilesExecutor executor)
                    && this.container.IsRegistered<ISendFilesExecutor>(cardInfo.ExecutorName))
                {
                    executor = this.container.Resolve<ISendFilesExecutor>(cardInfo.ExecutorName);
                    this.fileExecutors[cardInfo.ExecutorName] = executor;
                }

                if (executor == null)
                {
                    cardInfo.ErrorText = $"Не найден обработчик для отправки файлов с именем {cardInfo.ExecutorName}";
                    LogManager.GetCurrentClassLogger().Error("executor null ");
                    return;
                }
                var response = this.cardRepository.Get(new CardGetRequest { CardID = cardInfo.CardID });
                var valRes = response.ValidationResult.Build();
                if (!valRes.IsSuccessful)
                {
                    cardInfo.ErrorText = $"Ошибка при получении карточки: {valRes}";
                    logger.Error("cannot get card ");
                    return;
                }

                if (cardInfo.Storage != null) 
                { 
                    cardInfo.Storage["Attemps"] = cardInfo.Attempts; 
                }
                var result = executor.ProcessCard(response.Card, cardInfo.UserID, cardInfo.UserName, cardInfo.Storage);

                updateRes = this.UpdateFileVersion(this.dbScope, result, cardInfo.ExecutorName);

                if (!result.ValidationResultBuilder.IsSuccessful())
                {
                    cardInfo.ErrorText = $"Ошибка при обработке карточки: {result.ValidationResultBuilder.Build()}";
                    logger.Trace("result not succes: " + result.ValidationResultBuilder.ToString() + "\n " + result.ToString() );
                    return;
                }

                if (updateRes > 0 || updateRes == -2) // удалось удалить контент файла и обновить данные в таблице FileVersion или таск состоял из переиспользованных тасков
                {
                    successful = true;
                }
            }
            catch (Exception ex)
            {
                cardInfo.ErrorText = ex.ToString();
                logger.Trace(cardInfo.ErrorText);
                successful = false;
            }
            finally
            {
                if (successful)
                {
                    logger.Trace("Успешно завершена обработка записи {0} для карточки {1} с обработчиком {2}", cardInfo.ID, cardInfo.CardID, cardInfo.ExecutorName);
                    this.DeleteCardInfo(cardInfo);
                }
                else
                {
                    logger.Trace(cardInfo.ErrorText);
                    this.UpdateCardInfo(cardInfo);
                }
            }
        }

        /// <summary>
        /// Получает контент версии файла
        /// </summary>
        /// <param name="versionID"></param>
        /// <returns></returns>
        private CardContentContext GetSourceFileContent(Guid versionID)
        {
            using (this.dbScope.CreateNew())
            {
                var db = dbScope.Db;
                db.SetCommand(@"select

    f.ID
    , f.RowID
    , f.VersionRowID
	, fv.SourceID
    , f.Name
    , f.CategoryCaption
from Files f with(nolock) left join FileVersions fv with(nolock) on f.RowID = fv.ID
    where fv.ID = (select fv.ID
        from FileVersions fv with(nolock) left join FileVersions fv1 with(nolock) on fv.RowID = fv1.LinkID

        where fv1.RowID = @VersionID)
order by Number desc", db.Parameter("VersionID", versionID)).LogCommand();
                using (var reader = db.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return new CardContentContext(
                            reader.GetValue<Guid>(0),
                            reader.GetValue<Guid>(1),
                            reader.GetValue<Guid>(2),
                            new CardFileSourceType(reader.GetValue<short>(3)),
                            new ValidationResultBuilder()
                        );
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Получает контент версии файла
        /// </summary>
        /// <param name="versionID"></param>
        /// <returns></returns>
        private CardContentContext GetFileContent(Guid versionID)
        {
            using (this.dbScope.Create())
            {
                var db = dbScope.Db;
                db.SetCommand(@"
                     select 
                        f.ID,
                        f.RowID, 
                        f.VersionRowID, 
                        fv.SourceID,
                        f.Name,
                        f.CategoryCaption
                    from dbo.Files f with(nolock)
                    inner join dbo.FileVersions fv with(nolock) on f.VersionRowID = fv.RowID and fv.RowID = @versionID",
                    db.Parameter("@versionID", versionID)).LogCommand();

                using (var reader = this.dbScope.Db.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        return new CardContentContext(
                            reader.GetValue<Guid>(0),
                            reader.GetValue<Guid>(1),
                            reader.GetValue<Guid>(2),
                            new CardFileSourceType(reader.GetValue<short>(3)),
                            new ValidationResultBuilder()
                        );
                    }
                }
            }
            return null;
        }

        private void DeleteCardInfo(CardForProcess cardInfo)
        {
            logger.Trace("Удаление записи {0}", cardInfo.ID);
            using (this.dbScope.Create())
            {
                var executor = this.dbScope.Executor;
                executor.ExecuteNonQuery(DeleteCardAfterProcessingSql, executor.Parameter("ID", cardInfo.ID));
            }
        }

        private void UpdateCardInfo(CardForProcess cardInfo)
        {
            logger.Trace("Обновление записи {0}", cardInfo.ID);
            using (this.dbScope.Create())
            {
                var executor = this.dbScope.Executor;
                executor.ExecuteNonQuery(UpdateCardAfterProcessingSql,
                    executor.Parameter("ID", cardInfo.ID),
                    executor.Parameter("ErrorText", cardInfo.ErrorText),
                    executor.Parameter("Parameters", cardInfo.Parameters, System.Data.DbType.Binary));
            }
        }

        private List<CardForProcess> GetCardsForProcess(Guid processID)
        {
            using (this.dbScope.Create())
            {
                var db = this.dbScope.Db;
                return db.SetCommand(GetCardsForProcessSql, db.Parameter("ProcessID", processID))
                    .LogCommand()
                    .ExecuteList<CardForProcess>();
            }
        }

        protected override void SendFiles(Guid processID)
        {
            var cards = this.GetCardsForProcess(processID);
            logger.Trace("Найдено {0} карточек для обработки", cards.Count);
            foreach (var cardInfo in cards)
            {
                this.ProcessCard(cardInfo);
            }
        }

        #endregion

        #region Public Methods
        /// <summary>
        ///  после того как отработает сервис, он должен вернуть ID файла из базы dv, этот ID записываем в LinkID, меняем sourceID = 42, записываем option и удаляем запись из FileContent
        /// </summary>
        /// <param name="dbScope"></param>
        /// <param name="cardID"></param>
        /// <param name="linkID"></param>
        /// <param name="resolverName"></param>
        public int UpdateFileVersion(IDbScope dbScope, ProcessedFileInfo processedFileInfo, string baseExecutorName)
        {
            logger.Trace("Start UpdateFileVersion.");
            int subRes = -1;
            int currentSourceID = 0;
            int fileCatID;
            int sourceID;
            Guid linkID;
            Guid versionID;
            string executorName;
            if (processedFileInfo.processedFilesInfo.Count == 0)
            {
                logger.Trace("No processed files.");
                return -2;
            }
            foreach (var sf in processedFileInfo.processedFilesInfo)
            {
                executorName = baseExecutorName;
                linkID = sf.LinkID;
                versionID = sf.FileVersionID;
                sourceID = (int)ReusableFileSource.ProductDatabaseContent;
                currentSourceID = GetFileSourceID(versionID);
                logger.Trace($"Source id: {currentSourceID}");
                if (currentSourceID == -1)
                {
                    return -1;
                }
                if (executorName == ExternalFilesHelper.ExecutorNames.RkoSendFileExecutor)
                {
                    fileCatID = GetFileCatIDBySection(versionID);
                    logger.Trace($"File cat ID: {fileCatID}.");
                    if (fileCatID == RkoSolutionHelper.RkoFileCategories.Client)
                    {
                        executorName = ExternalFilesHelper.ResolverNames.ClientDossierResolver;
                        sourceID = (int)ReusableFileSource.ClientDatabaseContent;
                    }
                    else
                    {
                        //executorName = ExternalFilesHelper.ResolverNames.EAProductDossierResolver;
                        //sourceID = (int)ReusableFileSource.ProductDatabaseContent;
                        executorName = ExternalFilesHelper.ResolverNames.ProductPaperDossierResolver;
                        sourceID = (int)ReusableFileSource.ProductPaperContent;
                    }
                }
                else
                {
                    fileCatID = GetFileCatID(versionID);
                    logger.Trace($"File cat ID: {fileCatID}.");
                    if (fileCatID == RkoSolutionHelper.RkoFileCategories.Client)
                    {
                        sourceID = (int)ReusableFileSource.ClientDatabaseContent;
                    }
                }
                logger.Trace($"Executor name: {executorName}");
                var options = new FileOption()
                {
                    ResolverName = executorName
                };

                if (executorName == ExternalFilesHelper.ExecutorNames.EscrowSendFileExecutor)
                {
                    executorName = ExternalFilesHelper.ResolverNames.EAProductDossierResolver;
                }

                using (dbScope.Create())
                {
                    var db = dbScope.Db;
                    CardContentContext fileContent;
                    if (currentSourceID == (int)ReusableFileSource.CurrentDatabaseContent)
                    {
                        fileContent = this.GetSourceFileContent(versionID);
                        logger.Trace($"Got file content");
                        if (fileContent != null)
                        {
                            logger.Trace($"Source ID: {sourceID}");
                            logger.Trace($"Executor name: {executorName}");
                            logger.Trace("File content is not empty");
                            var contentStrategy = this.container.Resolve<ICardContentStrategy>();
                            contentStrategy.Delete(fileContent);
                            subRes = db.SetCommand(UpdateReusedFiles, db.Parameter("SourceID", sourceID)
                                , db.Parameter("LinkID", linkID), db.Parameter("VersionRowID", versionID)
                                , db.Parameter("options", FileOptionSerializer.JsonSerialize(options)))
                                .LogCommand().ExecuteNonQuery();
                            logger.Trace($"Reused subRes: {subRes}");
                            if (subRes < 1)
                            {
                                return subRes;
                            }
                            logger.Trace($"Source file version: {fileContent.VersionRowID}");
                            // считаем что удалось удалить файл из базы тесса / файловой системы 
                            subRes = db.SetCommand(UpdateSourceFile, db.Parameter("SourceID", sourceID)
                                    , db.Parameter("LinkID", linkID), db.Parameter("VersionRowID", fileContent.VersionRowID)
                                    , db.Parameter("options", FileOptionSerializer.JsonSerialize(options)))
                                .LogCommand().ExecuteNonQuery();
                            logger.Trace($"Source subRes: {subRes}");
                            if (subRes < 1)
                            {
                                return subRes;
                            }
                        }
                    }
                    else
                    {
                        fileContent = this.GetFileContent(versionID);
                        if (fileContent != null)
                        {
                            logger.Trace($"Source ID: {sourceID}");
                            logger.Trace($"Executor name: {executorName}");
                            var contentStrategy = this.container.Resolve<ICardContentStrategy>();
                            contentStrategy.Delete(fileContent);
                            // считаем что удалось удалить файл из базы тесса / файловой системы 
                            subRes = db.SetCommand(UpdateReusedFilesBySourceFileID, db.Parameter("SourceID", sourceID)
                                , db.Parameter("LinkID", linkID), db.Parameter("FileID", fileContent.FileID)
                                , db.Parameter("options", FileOptionSerializer.JsonSerialize(options)))
                                .LogCommand().ExecuteNonQuery();
                            logger.Trace($"Reused subRes: {subRes}");
                            subRes = db.SetCommand(@"update FileVersions
                                     set SourceID = @sourceID, LinkID = @LinkID, Options = @options
                                     where RowID = @versionID",
                                    db.Parameter("@sourceID", sourceID),
                                    db.Parameter("@LinkID", linkID),
                                    db.Parameter("@options", FileOptionSerializer.JsonSerialize(options)),
                                    db.Parameter("@versionID", versionID))
                                .LogCommand()
                                .ExecuteNonQuery();
                            logger.Trace($"Source subRes: {subRes}");
                            if (subRes < 1)
                            {
                                return subRes;
                            }
                        }
                    }
                }
            }
            return subRes;
        }
        #endregion

        #region IPlugin Members

        public override void EntryPoint()
        {
            logger.Trace("Starting FilesSenderPlugin");

            logger.Trace("Reading config parameters for FilesSenderPlugin");

            this.container = new UnityContainer().RegisterServerForPlugin();

            var processID = Guid.NewGuid();
            this.dbScope = this.container.Resolve<IDbScope>();

            using (this.dbScope.Create())
            {
                this.cardRepository = this.container.Resolve<ICardRepository>();
                this.fileExecutors = new Dictionary<string, ISendFilesExecutor>();
                this.BlockFiles(processID);
                this.SendFiles(processID);
            }
            logger.Trace("Finishing FilesSenderPlugin");
        }

        #endregion
    }
}
