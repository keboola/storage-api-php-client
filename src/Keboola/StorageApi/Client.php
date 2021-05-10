<?php
namespace Keboola\StorageApi;

use Aws\S3\S3Client;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\StorageApi\Options\BucketUpdateOptions;
use Keboola\StorageApi\Options\FileUploadTransferOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\IndexOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\Options\SearchTablesOptions;
use Keboola\StorageApi\Options\StatsOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use MicrosoftAzure\Storage\Blob\Models\CommitBlobBlocksOptions;
use MicrosoftAzure\Storage\Blob\Models\CreateBlockBlobOptions;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Options\FileUploadOptions;

class Client
{
    // Stage names
    const DEFAULT_RETRIES_COUNT = 15;
    const STAGE_IN = "in";
    const STAGE_OUT = "out";
    const STAGE_SYS = "sys";
    const API_VERSION = "v2";

    const VERSION = '10.4.0';

    const FILE_PROVIDER_AWS = 'aws';
    const FILE_PROVIDER_AZURE = 'azure';

    // Token string
    public $token;

    // current run id sent with all request
    private $runId = null;

    // API URL
    private $apiUrl;

    private $backoffMaxTries = 11;

    private $awsRetries = self::DEFAULT_RETRIES_COUNT;

    private $awsDebug = false;

    // User agent header send with each API request
    private $userAgent = 'Keboola Storage API PHP Client';

    /**
     * @var callable|null
     */
    private $jobPollRetryDelay;

    /**
     * @var LoggerInterface
     *
     */
    private $logger;

    /**
     * @var \GuzzleHttp\Client
     */
    private $client;

    /**
     *
     * Request timeout in seconds
     *
     * @var int
     */
    public $connectionTimeout = 7200;

    /** @var Tokens */
    private $tokens;

    /**
     * Clients accept an array of constructor parameters.
     *
     * Here's an example of creating a client using an URI template for the
     * client's base_url and an array of default request options to apply
     * to each request:
     *
     *     $client = new Client([
     *         'url' => 'https://connection.keboola.com'
     *         'token' => 'your_sapi_token',
     *     ]);
     *
     * @param array $config Client configuration settings
     *     - token: (required) Storage API token
     *     - url: (required) Storage API URL
     *     - userAgent: custom user agent
     *     - backoffMaxTries: backoff maximum number of attempts
     *     - awsRetries: number of aws client retries
     *     - logger: instance of Psr\Log\LoggerInterface
     *     - jobPollRetryDelay: callable method which determines wait period for job polling
     */
    public function __construct(array $config = array())
    {
        if (!isset($config['url'])) {
            throw new \InvalidArgumentException('url must be set');
        }
        $this->apiUrl = $config['url'];

        $this->userAgent .= '/' . self::VERSION;
        if (isset($config['userAgent'])) {
            $this->userAgent .= ' ' . $config['userAgent'];
        }

        if (!isset($config['token'])) {
            throw new \InvalidArgumentException('token must be set');
        }
        $this->token = $config['token'];

        if (isset($config['backoffMaxTries'])) {
            $this->backoffMaxTries = (int)$config['backoffMaxTries'];
        }

        if (isset($config['awsRetries'])) {
            $this->awsRetries = (int)$config['awsRetries'];
        }

        if (isset($config['awsDebug'])) {
            $this->awsDebug = (bool)$config['awsDebug'];
        }

        if (!isset($config['logger'])) {
            $config['logger'] = new NullLogger();
        }
        $this->setLogger($config['logger']);

        if (isset($config['jobPollRetryDelay'])) {
            $this->setJobPollRetryDelay($config['jobPollRetryDelay']);
        } else {
            $this->setJobPollRetryDelay(createSimpleJobPollDelay());
        }

        $this->initClient();
        $this->tokens = new Tokens($this);
    }

    private function initClient()
    {
        $handlerStack = HandlerStack::create([
            'backoffMaxTries' => $this->backoffMaxTries,
        ]);

        $handlerStack->push(Middleware::log(
            $this->logger,
            new MessageFormatter("{hostname} {req_header_User-Agent} - [{ts}] \"{method} {resource} {protocol}/{version}\" {code} {res_header_Content-Length}"),
            LogLevel::DEBUG
        ));
        $this->client = new \GuzzleHttp\Client([
            'base_uri' => $this->apiUrl,
            'handler' => $handlerStack,
        ]);
    }

    private function setJobPollRetryDelay(callable $jobPollRetryDelay)
    {
        $this->jobPollRetryDelay = $jobPollRetryDelay;
    }

    /**
     * Get API Url
     *
     * @return string
     */
    public function getApiUrl()
    {
        return $this->apiUrl;
    }

    /**
     * Return URL of the service from API index.
     *
     * @param string $serviceName
     * @return string
     * @throws ClientException
     */
    public function getServiceUrl($serviceName)
    {
        $indexResult = $this->indexAction();

        if (!isset($indexResult['services']) || !is_array($indexResult['services'])) {
            throw new ClientException('API index is missing "services" section');
        }

        foreach ($indexResult['services'] as $service) {
            if (!isset($service['id']) || $service['id'] !== $serviceName) {
                continue;
            }

            if (!isset($service['url'])) {
                throw new ClientException(sprintf('Definition of service "%s" is missing URL', $serviceName));
            }

            return $service['url'];
        }

        throw new ClientException(sprintf('No service with ID "%s" found', $serviceName));
    }

    /**
     * API index with available components list
     * @return array
     */
    public function indexAction(IndexOptions $options = null)
    {
        $url = '';

        if ($options !== null) {
            $url .= '?' . http_build_query($options->toArray());
        }
        return $this->apiGet($url);
    }

    public function webalizeDisplayName($displayName)
    {
        return $this->apiPostJson(
            'webalize/display-name',
            ['displayName' => $displayName]
        );
    }

    /**
     * Get UserAgent name
     *
     * @return string
     */
    public function getUserAgent()
    {
        return $this->userAgent;
    }

    /**
     *
     * List all buckets
     *
     * @return array
     */
    public function listBuckets($options = array())
    {
        return $this->apiGet("buckets?" . http_build_query($options));
    }

    /**
     *
     * Get bucket id from name and stage
     *
     * @param string $name
     * @param string $stage
     * @return bool|string
     */
    public function getBucketId($name, $stage)
    {
        $buckets = $this->listBuckets();
        foreach ($buckets as $bucket) {
            if ($bucket["stage"] == $stage && $bucket["name"] == $name) {
                return $bucket["id"];
            }
        }
        return false;
    }

    /**
     *
     * Bucket details
     *
     * @param string $bucketId
     * @return array
     */
    public function getBucket($bucketId)
    {
        return $this->apiGet("buckets/" . $bucketId);
    }

    /**
     * @param BucketUpdateOptions $options
     * @return array
     */
    public function updateBucket(BucketUpdateOptions $options)
    {
        return $this->apiPut('buckets/' . $options->getBucketId(), $options->toParamsArray());
    }

    /**
     *
     * Create a bucket. If a bucket exists, return existing bucket URL.
     *
     * @param string $name bucket name
     * @param string $stage bucket stage
     * @param string $description bucket description
     * @param string|null $displayName name that will be displayed in the UI - can be changed
     *
     * @return string bucket Id
     */
    public function createBucket($name, $stage, $description = "", $backend = null, $displayName = null)
    {
        $options = array(
            "name" => $name,
            "stage" => $stage,
            "description" => $description,
        );

        if ($backend) {
            $options['backend'] = $backend;
        }

        if ($displayName) {
            $options['displayName'] = $displayName;
        }

        $bucketId = $this->getBucketId($name, $stage);
        if ($bucketId) {
            return $bucketId;
        }

        $result = $this->apiPost("buckets", $options);

        $this->log("Bucket {$result["id"]} created", array("options" => $options, "result" => $result));

        return $result["id"];
    }

    /**
     * Link shared bucket to project
     *
     * @param string $name new bucket name
     * @param string $stage bucket stage
     * @param int $sourceProjectId
     * @param int $sourceBucketId
     * @param string|null $displayName bucket display name
     * @return mixed
     */
    public function linkBucket($name, $stage, $sourceProjectId, $sourceBucketId, $displayName = null)
    {
        $options = array(
            "name" => $name,
            "stage" => $stage,
            "sourceProjectId" => $sourceProjectId,
            "sourceBucketId" => $sourceBucketId,
        );

        if ($displayName) {
            $options['displayName'] = $displayName;
        }

        $result = $this->apiPost("buckets", $options);

        $this->log("Shared bucket {$result["id"]} linked to the project", array("options" => $options, "result" => $result));

        return $result["id"];
    }

    /**
     *
     * Delete a bucket. Only empty buckets can be deleted
     *
     * @param string $bucketId
     * @param array $options - (bool) force
     * @return mixed|string
     */
    public function dropBucket($bucketId, $options = array())
    {
        $url = "buckets/" . $bucketId;

        $allowedOptions = array(
            'force',
            'async'
        );

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        $url .= '?' . http_build_query($filteredOptions);

        return $this->apiDelete($url);
    }

    public function shareBucket($bucketId, $options = [])
    {
        $url = "buckets/" . $bucketId . "/share";
        $url .= '?' . http_build_query($options);

        $result = $this->apiPost($url, [], false);

        $this->log("Bucket {$bucketId} shared", array("result" => $result));

        return $result;
    }

    public function shareOrganizationBucket($bucketId)
    {
        $url = "buckets/" . $bucketId . "/share-organization";

        $result = $this->apiPost($url, [], false);

        $this->log("Bucket {$bucketId} shared", ["result" => $result]);

        return $result;
    }

    public function shareOrganizationProjectBucket($bucketId)
    {
        $url = "buckets/" . $bucketId . "/share-organization-project";

        $result = $this->apiPost($url, [], false);

        $this->log("Bucket {$bucketId} shared", ["result" => $result]);

        return $result;
    }

    public function shareBucketToProjects($bucketId, $targetProjectIds)
    {
        $url = "buckets/" . $bucketId . "/share-to-projects";
        $url .= '?' . http_build_query(['targetProjectIds' => $targetProjectIds]);

        $result = $this->apiPost($url, [], false);

        $this->log("Bucket {$bucketId} shared", ["result" => $result]);

        return $result;
    }

    public function shareBucketToUsers($bucketId, $targetUsers = [])
    {
        $url = "buckets/" . $bucketId . "/share-to-users";
        $url .= '?' . http_build_query(['targetUsers' => $targetUsers]);

        $result = $this->apiPost($url, [], false);

        $this->log("Bucket {$bucketId} shared", ["result" => $result]);

        return $result;
    }

    public function changeBucketSharing($bucketId, $sharing)
    {
        $url = "buckets/" . $bucketId . "/share";

        $result = $this->apiPut($url, ['sharing' => $sharing]);

        $this->log("Bucket {$bucketId} sharing changed to {$sharing}", array("result" => $result));

        return $result;
    }

    public function unshareBucket($bucketId)
    {
        $url = "buckets/" . $bucketId . "/share";

        return $this->apiDelete($url);
    }

    public function forceUnlinkBucket($bucketId, $projectId, $options = [])
    {

        $url = "buckets/" . $bucketId . "/links/" . $projectId;

        $allowedOptions = [
            'async',
        ];

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        $url .= '?' . http_build_query($filteredOptions);

        return $this->apiDelete($url);
    }

    public function isSharedBucket($bucketId)
    {
        $url = "buckets/" . $bucketId;

        $result = $this->apiGet($url);

        return !empty($result['sharing']);
    }

    public function listSharedBuckets($options = [])
    {
        $url = "shared-buckets";

        $allowedOptions = [
            'include',
        ];

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        if (!empty($filteredOptions)) {
            $url .= '?' . http_build_query($filteredOptions);
        }

        return $this->apiGet($url);
    }

    /**
     *
     * Set a bucket attribute
     *
     * @deprecated
     * @param string $bucketId
     * @param string $key
     * @param string $value
     * @param bool null $protected
     */
    public function setBucketAttribute($bucketId, $key, $value, $protected = null)
    {
        $data = array(
            'value' => $value,
        );
        if ($protected !== null) {
            $data['protected'] = (bool)$protected;
        }
        $this->apiPost("buckets/$bucketId/attributes/$key", $data);
    }

    /**
     * @deprecated
     * @param $bucketId
     * @param array $attributes array of objects with `name`, `value`, `protected` keys
     */
    public function replaceBucketAttributes($bucketId, $attributes = array())
    {
        $params = array();
        if (!empty($attributes)) {
            $params['attributes'] = $attributes;
        }
        $this->apiPost("buckets/$bucketId/attributes", $params);
    }


    /**
     *
     * Delete a bucket attribute
     *
     * @deprecated
     * @param string $bucketId
     * @param string $key
     * @return mixed|string
     */
    public function deleteBucketAttribute($bucketId, $key)
    {
        $result = $this->apiDelete("buckets/$bucketId/attributes/$key");
        $this->log("Bucket $bucketId attribute $key deleted");
        return $result;
    }

    /**
     *
     * Checks if a bucket exists
     *
     * @param string $bucketId
     * @return bool
     * @throws ClientException
     * @throws \Exception
     */
    public function bucketExists($bucketId)
    {
        try {
            $this->getBucket($bucketId);
            return true;
        } catch (ClientException $e) {
            if ($e->getCode() == 404) {
                return false;
            }
            throw $e;
        }
    }

    /**
     * @param $bucketId
     * @param $name
     * @param CsvFile $csvFile
     * @param array $options
     *  - primaryKey - string, multiple column primary keys separate by comma
     * @return string - created table id
     */
    public function createTable($bucketId, $name, CsvFile $csvFile, $options = array())
    {
        $options = array(
            "bucketId" => $bucketId,
            "name" => $name,
            "delimiter" => $csvFile->getDelimiter(),
            "enclosure" => $csvFile->getEnclosure(),
            "escapedBy" => $csvFile->getEscapedBy(),
            "primaryKey" => isset($options['primaryKey']) ? $options['primaryKey'] : null,
            "columns" => isset($options['columns']) ? $options['columns'] : null,
            "data" => fopen($csvFile->getPathname(), 'r'),
            'syntheticPrimaryKeyEnabled' => isset($options['syntheticPrimaryKeyEnabled']) ? $options['syntheticPrimaryKeyEnabled'] : null,
        );


        $tableId = $this->getTableId($name, $bucketId);
        if ($tableId) {
            return $tableId;
        }
        $result = $this->apiPostMultipart("buckets/" . $bucketId . "/tables", $this->prepareMultipartData($options));

        $this->log("Table {$result["id"]} created", array("options" => $options, "result" => $result));

        if (!empty($options['data']) && is_resource($options['data'])) {
            fclose($options['data']);
        }
        return $result["id"];
    }

    /**
     * Creates table with header of CSV file, then import whole csv file by async import
     * Handles async operation. Starts import job and waits when it is finished. Throws exception if job finishes with error.
     *
     * Workflow:
     *  - Upload file to File Uploads
     *  - Initialize table import with previously uploaded file
     *  - Wait until job is finished
     *  - Return created table id
     *
     * @param $bucketId
     * @param $name
     * @param CsvFile $csvFile
     * @param array $options - see createTable method params
     * @return string - created table id
     */
    public function createTableAsync($bucketId, $name, CsvFile $csvFile, $options = array())
    {
        $options = array(
            "bucketId" => $bucketId,
            "name" => $name,
            "delimiter" => $csvFile->getDelimiter(),
            "enclosure" => $csvFile->getEnclosure(),
            "escapedBy" => $csvFile->getEscapedBy(),
            "primaryKey" => isset($options['primaryKey']) ? $options['primaryKey'] : null,
            "distributionKey" => isset($options['distributionKey']) ? $options['distributionKey'] : null,
            "transactional" => isset($options['transactional']) ? $options['transactional'] : false,
            'columns' => isset($options['columns']) ? $options['columns'] : null,
            'syntheticPrimaryKeyEnabled' => isset($options['syntheticPrimaryKeyEnabled']) ? $options['syntheticPrimaryKeyEnabled'] : null,
        );

        // upload file
        $fileId = $this->uploadFile(
            $csvFile->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(array('file-import'))
        );
        $options['dataFileId'] = $fileId;

        return $this->createTableAsyncDirect($bucketId, $options);
    }

    /**
     * Starts and waits for async table import.
     *
     *
     * @param $bucketId
     * @param array $options see createTable method params
     * @return string - created table id
     */
    public function createTableAsyncDirect($bucketId, $options = array())
    {
        $createdTable = $this->apiPost("buckets/{$bucketId}/tables-async", $options);
        return $createdTable['id'];
    }

    /**
     * @param $bucketId destination bucket
     * @param $snapshotId source snapshot
     * @param null $name table name (optional) otherwise fetched from snapshot
     * @return string - created table id
     */
    public function createTableFromSnapshot($bucketId, $snapshotId, $name = null)
    {
        return $this->createTableAsyncDirect($bucketId, array(
            'snapshotId' => $snapshotId,
            'name' => $name,
        ));
    }

    /**
     * @param $bucketId string destination bucket
     * @param $sourceTableId string source snapshot
     * @param $timestamp string timestamp to use for table replication
     * @param $name string table name
     * @return string - created table id
     */
    public function createTableFromSourceTableAtTimestamp($bucketId, $sourceTableId, $timestamp, $name)
    {
        return $this->createTableAsyncDirect($bucketId, array(
            'sourceTableId' => $sourceTableId,
            'timestamp' => $timestamp,
            'name' => $name,
        ));
    }

    /**
     * @param $bucketId
     * @param $sourceTableId
     * @param null $name
     * @param array $options
     *  - sourceTable
     *  - name (optional)
     *  - aliasFilter (optional)
     *  - (array) aliasColumns (optional)
     * @return string  - created table id
     */
    public function createAliasTable($bucketId, $sourceTableId, $name = null, $options = array())
    {
        $filteredOptions = array(
            'sourceTable' => $sourceTableId,
            'name' => $name,
        );

        if (isset($options['aliasFilter'])) {
            $filteredOptions['aliasFilter'] = (array)$options['aliasFilter'];
        }

        if (isset($options['aliasColumns'])) {
            $filteredOptions['aliasColumns'] = (array)$options['aliasColumns'];
        }

        $result = $this->apiPost("buckets/" . $bucketId . "/table-aliases", $filteredOptions);
        $this->log("Table alias {$result["id"]}  created", array("options" => $filteredOptions, "result" => $result));
        return $result["id"];
    }

    /**
     * @param $tableId
     * @return int - snapshot id
     */
    public function createTableSnapshot($tableId, $snapshotDescription = null)
    {
        $result = $this->apiPost("tables/{$tableId}/snapshots", array(
            'description' => $snapshotDescription,
        ));
        $this->log("Snapthos {$result['id']} of table {$tableId} created.");
        return $result["id"];
    }

    /**
     * @param string $tableId
     * @param array $options
     * @return string
     */
    public function updateTable($tableId, $options)
    {
        $allowedOptions = [
            'displayName',
            'async'
        ];

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        $result = $this->apiPut('tables/' . $tableId, $filteredOptions);
        $this->log("Table {$tableId} updated");
        return $result['id'];
    }

    /**
     * @param $tableId
     * @return mixed|string
     */
    public function listTableSnapshots($tableId, $options = array())
    {
        return $this->apiGet("tables/{$tableId}/snapshots?" . http_build_query($options));
    }

    /**
     * @param $tableId
     * @param array $filter
     * @return mixed|string
     */
    public function setAliasTableFilter($tableId, array $filter)
    {
        $result = $this->apiPost("tables/$tableId/alias-filter", $filter);
        $this->log("Table $tableId  filter set", array(
            'filter' => $filter,
            'result' => $result,
        ));
        return $result;
    }

    public function removeAliasTableFilter($tableId)
    {
        $this->apiDelete("tables/$tableId/alias-filter");
    }

    /**
     * @param $tableId
     */
    public function enableAliasTableColumnsAutoSync($tableId)
    {
        $this->apiPost("tables/{$tableId}/alias-columns-auto-sync");
    }

    /**
     * @param $tableId
     */
    public function disableAliasTableColumnsAutoSync($tableId)
    {
        $this->apiDelete("tables/{$tableId}/alias-columns-auto-sync");
    }

    /**
     *
     * Get all available tables
     *
     * @param string $bucketId limit search to a specific bucket
     * @param array $options
     * @return array
     */
    public function listTables($bucketId = null, $options = array())
    {
        if ($bucketId) {
            return $this->apiGet("buckets/{$bucketId}/tables?" . http_build_query($options));
        }
        return $this->apiGet("tables?" . http_build_query($options));
    }

    /**
     *
     * Gets the table id from bucket id and table name
     *
     * @param string $name
     * @param string $bucketId
     * @return bool|string table id or false
     */
    public function getTableId($name, $bucketId)
    {
        $tables = $this->listTables($bucketId);
        foreach ($tables as $table) {
            if ($table["name"] == $name) {
                return $table["id"];
            }
        }
        return false;
    }

    /**
     * @param $tableId
     * @param CsvFile $csvFile
     * @param array $options
     *    Available options:
     *  - incremental
     *  - delimiter
     *  - enclosure
     *  - escapedBy
     *  - dataFileId
     *  - dataTableName
     *  - dataWorkspaceId
     *  - data
     *  - withoutHeaders
     *  - columns
     * @return mixed|string
     * @throws ClientException
     */
    public function writeTable($tableId, CsvFile $csvFile, $options = array())
    {
        $optionsExtended = $this->writeTableOptionsPrepare(array_merge($options, array(
            "delimiter" => $csvFile->getDelimiter(),
            "enclosure" => $csvFile->getEnclosure(),
            "escapedBy" => $csvFile->getEscapedBy(),
        )));

        $optionsExtended["data"] = @fopen($csvFile->getRealPath(), 'r');
        if ($optionsExtended["data"] === false) {
            throw new ClientException("Failed to open temporary data file " . $csvFile->getRealPath(), null, null, 'fileNotReadable');
        }

        $result = $this->apiPostMultipart("tables/{$tableId}/import", $this->prepareMultipartData($optionsExtended));

        $this->log("Data written to table {$tableId}", array("options" => $optionsExtended, "result" => $result));
        return $result;
    }

    /**
     * Write data into table asynchronously and wait for result
     *
     * @param $tableId
     * @param CsvFile $csvFile
     * @param array $options
     * @return array - table write results
     */
    public function writeTableAsync($tableId, CsvFile $csvFile, $options = array())
    {
        $optionsExtended = array_merge($options, array(
            "delimiter" => $csvFile->getDelimiter(),
            "enclosure" => $csvFile->getEnclosure(),
            "escapedBy" => $csvFile->getEscapedBy(),
        ));

        // upload file
        $fileId = $this->uploadFile(
            $csvFile->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(array('table-import'))
        );
        $optionsExtended['dataFileId'] = $fileId;

        return $this->writeTableAsyncDirect($tableId, $optionsExtended);
    }

    /**
     * Performs asynchronous write and waits for result
     * Executes http://docs.keboola.apiary.io/#post-%2Fv2%2Fstorage%2Fbuckets%2F%7Bbucket_id%7D%2Ftables-async
     * @param $tableId
     * @param array $options
     * @return array
     */
    public function writeTableAsyncDirect($tableId, $options = array())
    {
        return $this->apiPost("tables/{$tableId}/import-async", $this->writeTableOptionsPrepare($options));
    }

    /**
     * @param $tableId
     * @param array $options
     * @return int
     */
    public function queueTableImport($tableId, $options = array())
    {
        $job = $this->apiPost("tables/{$tableId}/import-async", $this->writeTableOptionsPrepare($options), false);
        return $job["id"];
    }

    /**
     * @param $tableId
     * @param $options
     * @return int
     */
    public function queueTableExport($tableId, $options = array())
    {
        $job = $this->apiPost("tables/{$tableId}/export-async", $this->prepareExportOptions($options), false);
        return $job["id"];
    }

    private function writeTableOptionsPrepare($options)
    {
        $allowedOptions = array(
            'delimiter',
            'enclosure',
            'escapedBy',
            'dataFileId',
            'dataTableName',
            'dataObject',
            'dataWorkspaceId',
            'data',
            'withoutHeaders',
            'columns',
        );

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        return array_merge($filteredOptions, array(
            "incremental" => isset($options['incremental']) ? (bool)$options['incremental'] : false,
        ));
    }

    /**
     *
     * Get table details
     *
     * @param string $tableId
     * @return array
     */
    public function getTable($tableId)
    {
        return $this->apiGet("tables/" . $tableId);
    }

    /**
     *
     * Drop a table
     *
     * @param string $tableId
     * @param array $options - (bool) force
     * @return mixed|string
     */
    public function dropTable($tableId, $options = array())
    {
        $url = "tables/" . $tableId;

        $allowedOptions = array(
            'force',
        );

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        $url .= '?' . http_build_query($filteredOptions);

        $result = $this->apiDelete($url);
        $this->log("Table {$tableId} deleted");
        return $result;
    }

    /**
     *
     * Set a table attribute
     *
     * @deprecated
     * @param string $tableId
     * @param string $key
     * @param string $value
     * @param bool null $protected
     */
    public function setTableAttribute($tableId, $key, $value, $protected = null)
    {
        $data = array(
            'value' => $value,
        );
        if ($protected !== null) {
            $data['protected'] = (bool)$protected;
        }
        $this->apiPost("tables/$tableId/attributes/$key", $data);
    }

    /**
     * @deprecated
     * @param $tableId
     * @param array $attributes array of objects with `name`, `value`, `protected` keys
     */
    public function replaceTableAttributes($tableId, $attributes = array())
    {
        $params = array();
        if (!empty($attributes)) {
            $params['attributes'] = $attributes;
        }
        $this->apiPost("tables/$tableId/attributes", $params);
    }

    /**
     *
     * Delete a table attribute
     *
     * @deprecated
     * @param string $tableId
     * @param string $key
     * @return mixed|string
     */
    public function deleteTableAttribute($tableId, $key)
    {
        $result = $this->apiDelete("tables/$tableId/attributes/$key");
        $this->log("Table $tableId attribute $key deleted");
        return $result;
    }

    /**
     *
     * Add column to table
     *
     * @param string $tableId
     * @param string $name
     */
    public function addTableColumn($tableId, $name)
    {
        $data = array(
            'name' => $name,
        );
        $this->apiPost("tables/$tableId/columns", $data);
    }


    /**
     *
     * Delete a table attribute
     *
     * @param string $tableId
     * @param string $name
     * @param array $options - (bool) force
     * @return mixed|string
     */
    public function deleteTableColumn($tableId, $name, $options = array())
    {
        $url = "tables/$tableId/columns/$name";

        $allowedOptions = array(
            'force',
        );

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        $url .= '?' . http_build_query($filteredOptions);

        $this->apiDelete($url);
        $this->log("Table $tableId column $name deleted");
    }

    /**
     *
     * Checks if a table exists
     *
     * @param string $tableId
     * @return bool
     * @throws ClientException
     * @throws \Exception
     */
    public function tableExists($tableId)
    {
        try {
            $this->getTable($tableId);
            return true;
        } catch (ClientException $e) {
            if ($e->getCode() == 404) {
                return false;
            }
            throw $e;
        }
    }

    /**
     * @param SearchTablesOptions $options
     * @return array
     * @throws \Exception
     */
    public function searchTables(SearchTablesOptions $options)
    {
        return $this->apiGet("search/tables?" . http_build_query($options->toArray()));
    }

    /**
     * @param $jobId
     * @return array
     */
    public function getJob($jobId)
    {
        return $this->apiGet("jobs/" . $jobId);
    }

    public function listJobs($options = [])
    {
        return $this->apiGet("jobs?" . http_build_query($options));
    }

    /**
     *
     * returns all tokens
     *
     * @return array
     * @deprecated Will be removed in next major release. Use Tokens::listTokens()
     */
    public function listTokens()
    {
        return $this->tokens->listTokens();
    }

    /**
     *
     * get token detail
     *
     * @param string $tokenId token id
     * @return array
     * @deprecated Will be removed in next major release. Use Tokens::getToken()
     */
    public function getToken($tokenId)
    {
        return $this->tokens->getToken($tokenId);
    }

    /**
     *
     * Returns the token string
     *
     * @return string
     */
    public function getTokenString()
    {
        return $this->token;
    }

    /**
     *
     * Verify the token
     *
     * @return mixed|string
     */
    public function verifyToken()
    {
        return $this->apiGet("tokens/verify");
    }

    /**
     * @throws ClientException
     * @deprecated will be removed in v11
     */
    public function getKeenReadCredentials()
    {
        throw new ClientException('Api endpoint \'storage/tokens/keen\' was removed from KBC');
    }

    /**
     * @deprecated Will be removed in next major release. Use Tokens::createToken()
     */
    public function createToken(TokenCreateOptions $options)
    {
        $result = $this->tokens->createToken($options);

        $this->log("Token {$result["id"]} created", ["options" => $options->toParamsArray(), "result" => $result]);

        return $result["id"];
    }

    /**
     *
     * update token details
     *
     * @param TokenUpdateOptions $options
     * @return int token id
     * @deprecated Will be removed in next major release. Use Tokens::updateToken()
     */
    public function updateToken(TokenUpdateOptions $options)
    {
        $result = $this->tokens->updateToken($options);

        $this->log("Token {$options->getTokenId()} updated", [
            "options" => $options->toParamsArray(),
            "result" => $result
        ]);

        return $result['id'];
    }

    /**
     * @param string $tokenId
     * @deprecated Will be removed in next major release. Use Tokens::dropToken()
     */
    public function dropToken($tokenId)
    {
        $this->tokens->dropToken($tokenId);
        $this->log("Token {$tokenId} deleted");
        return ''; // BC
    }

    /**
     *
     * Refreshes a token. If refreshing current token, the token is updated.
     *
     * @param string $tokenId If not set, defaults to self
     * @return string new token
     * @deprecated $tokenId parameter will be removed in next major release. Use Tokens::refreshToken()
     */
    public function refreshToken($tokenId = null)
    {
        $currentToken = $this->verifyToken();
        if ($tokenId == null) {
            $tokenId = $currentToken["id"];
        }

        $result = $this->tokens->refreshToken($tokenId);

        if ($currentToken["id"] == $result["id"]) {
            $this->token = $result['token'];
        }

        $this->log("Token {$tokenId} refreshed", array("token" => $result));

        return $result["token"];
    }

    /**
     * @param $tokenId
     * @param $recipientEmail
     * @param $message
     * @deprecated Will be removed in next major release. Use Tokens::shareToken()
     */
    public function shareToken($tokenId, $recipientEmail, $message)
    {
        $this->tokens->shareToken($tokenId, $recipientEmail, $message);
    }


    /**
     * Table data preview
     *
     * @param string $tableId
     * @param array $options all options are optional
     *    - (int) limit,
     *  - (timestamp | strtotime format) changedSince
     *  - (timestamp | strtotime format) changedUntil
     *  - (bool) escape
     *  - (array) columns
     *
     * @return string
     */
    public function getTableDataPreview($tableId, $options = array())
    {
        $url = "tables/{$tableId}/data-preview";
        $url .= '?' . http_build_query($this->prepareExportOptions($options));

        return $this->apiGet($url);
    }

    /**
     * Exports table content into File Uploads asynchronously. Waits for async operation result. Created file id is stored in returned job results.
     * http://docs.keboola.apiary.io/#post-%2Fv2%2Fstorage%2Ftables%2F%7Btable_id%7D%2Fexport-async
     *
     * @param $tableId
     * @param array $options
     *    - (int) limit,
     *  - (timestamp | strtotime format) changedSince
     *  - (timestamp | strtotime format) changedUntil
     *  - (bool) escape
     *  - (array) columns
     * @return array job results
     */
    public function exportTableAsync($tableId, $options = array())
    {
        return $this->apiPost(
            "tables/{$tableId}/export-async",
            $this->prepareExportOptions($options)
        );
    }

    private function prepareExportOptions(array $options)
    {
        $allowedOptions = [
            'limit',
            'changedSince',
            'changedUntil',
            'escape',
            'format',
            'whereColumn',
            'whereOperator',
            'gzip',
            'whereFilters',
            'orderBy',
            'fulltextSearch',
        ];

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        if (isset($options['columns'])) {
            $filteredOptions['columns'] = implode(',', (array)$options['columns']);
        }

        if (isset($options['whereValues'])) {
            $filteredOptions['whereValues'] = (array)$options['whereValues'];
        }

        return $filteredOptions;
    }

    /**
     * @param $tableId
     * @param array $options - (int) limit, (timestamp | strtotime format) changedSince, (timestamp | strtotime format) changedUntil, (bool) escape, (array) columns
     * @return mixed|string
     */
    public function deleteTableRows($tableId, $options = array())
    {
        $url = "tables/{$tableId}/rows";

        $allowedOptions = array(
            'changedSince',
            'changedUntil',
            'whereColumn',
            'whereOperator'
        );

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        if (isset($options['whereValues'])) {
            $filteredOptions['whereValues'] = (array)$options['whereValues'];
        }

        $url .= '?' . http_build_query($filteredOptions);

        return $this->apiDelete($url);
    }

    /**
     * Upload a file to file uploads
     *
     * @param $filePath
     * @param FileUploadOptions $options
     * @param FileUploadTransferOptions|null $transferOptions
     * @return int - created file id
     * @throws ClientException
     */
    public function uploadFile($filePath, FileUploadOptions $options, FileUploadTransferOptions $transferOptions = null)
    {
        if (!is_readable($filePath)) {
            throw new ClientException("File is not readable: " . $filePath, null, null, 'fileNotReadable');
        }
        $newOptions = clone $options;
        $fs = null;
        $currentUploadDir = null;
        if ($newOptions->getCompress() && !in_array(strtolower(pathinfo($filePath, PATHINFO_EXTENSION)), array("gzip", "gz", "zip"))) {
            $fs = new Filesystem();
            $sapiClientTempDir = sys_get_temp_dir() . '/sapi-php-client';
            if (!$fs->exists($sapiClientTempDir)) {
                $fs->mkdir($sapiClientTempDir);
            }

            $currentUploadDir = $sapiClientTempDir . '/' . uniqid('file-upload');
            $fs->mkdir($currentUploadDir);

            // gzip file and preserve it's base name
            $gzFilePath = $currentUploadDir . '/' . basename($filePath) . '.gz';
            $command = sprintf("gzip -c %s > %s", escapeshellarg($filePath), escapeshellarg($gzFilePath));

            $process = ProcessPolyfill::createProcess($command);
            $process->setTimeout(null);
            if (0 !== $process->run()) {
                $error = sprintf(
                    'The command "%s" failed.' . "\nExit Code: %s(%s)",
                    $process->getCommandLine(),
                    $process->getExitCode(),
                    $process->getExitCodeText()
                );
                throw new ClientException("Failed to gzip file. " . $error);
            }
            $filePath = $gzFilePath;
        }
        $newOptions
            ->setFileName(basename($filePath))
            ->setSizeBytes(filesize($filePath))
            ->setFederationToken(true);

        $prepareResult = $this->prepareFileUpload($newOptions);

        switch ($prepareResult['provider']) {
            case self::FILE_PROVIDER_AZURE:
                $this->uploadFileToAbs(
                    $prepareResult,
                    $filePath
                );
                break;
            case self::FILE_PROVIDER_AWS:
                $this->uploadFileToS3(
                    $prepareResult,
                    $filePath,
                    $newOptions,
                    $transferOptions
                );
        }

        if ($fs) {
            $fs->remove($currentUploadDir);
        }

        return $prepareResult['id'];
    }

    /**
     * @param array $prepareResult
     * @param string $filePath
     */
    private function uploadFileToAbs(
        array $prepareResult,
        $filePath
    ) {
        $options = new CommitBlobBlocksOptions();
        $options->setContentDisposition(
            sprintf('attachment; filename=%s', $prepareResult['name'])
        );
        $blobClient = BlobClientFactory::createClientFromConnectionString(
            $prepareResult['absUploadParams']['absCredentials']['SASConnectionString']
        );

        $uploader = new ABSUploader($blobClient);
        $uploader->uploadFile(
            $prepareResult['absUploadParams']['container'],
            $prepareResult['absUploadParams']['blobName'],
            $filePath,
            $options
        );
    }

    /**
     * @param array $prepareResult
     * @param string $filePath
     * @param FileUploadOptions $newOptions
     * @param FileUploadTransferOptions|null $transferOptions
     * @throws ClientException
     */
    private function uploadFileToS3(
        array $prepareResult,
        $filePath,
        FileUploadOptions $newOptions,
        FileUploadTransferOptions $transferOptions = null
    ) {
        $uploadParams = $prepareResult['uploadParams'];
        $s3options = [
            'version' => '2006-03-01',
            'retries' => $this->getAwsRetries(),
            'region' => $prepareResult['region'],
            'debug' => false,
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ]
        ];

        if ($this->isAwsDebug()) {
            $logfn = function ($message) {
                if (trim($message) != '') {
                    $this->log($message, ['source' => 'AWS SDK PHP debug']);
                }
            };
            $s3options['debug'] = [
                'logfn' => function ($message) use ($logfn) {
                    call_user_func($logfn, $message);
                },
                'stream_size' => 0,
                'scrub_auth' => true,
                'http' => true
            ];
        }

        $s3Client = new S3Client($s3options);
        $s3Uploader = new S3Uploader($s3Client, $transferOptions);
        $s3Uploader->uploadFile(
            $uploadParams['bucket'],
            $uploadParams['key'],
            $uploadParams['acl'],
            $filePath,
            $prepareResult['name'],
            $newOptions->getIsEncrypted() ? $uploadParams['x-amz-server-side-encryption'] : null
        );
    }

    /**
     * Upload a sliced file to file uploads. This method ignores FileUploadOption->getMultipartThreshold().
     *
     * @param array $slices list of slices that make the file
     * @param FileUploadOptions $options
     * @param FileUploadTransferOptions $transferOptions
     * @return int created file id
     * @throws ClientException
     */
    public function uploadSlicedFile(array $slices, FileUploadOptions $options, FileUploadTransferOptions $transferOptions = null)
    {
        if (!$options->getIsSliced()) {
            throw new ClientException("File is not sliced.");
        }
        if (!$options->getFileName()) {
            throw new ClientException("File name for sliced file upload not set.");
        }

        $newOptions = clone $options;
        $fs = null;
        $currentUploadDir = null;
        $fs = new Filesystem();
        $sapiClientTempDir = sys_get_temp_dir() . '/sapi-php-client';
        if (!$fs->exists($sapiClientTempDir)) {
            $fs->mkdir($sapiClientTempDir);
        }
        $currentUploadDir = $sapiClientTempDir . '/' . uniqid('file-upload');
        $fs->mkdir($currentUploadDir);

        if ($newOptions->getCompress()) {
            foreach ($slices as $key => $filePath) {
                if (!in_array(strtolower(pathinfo($filePath, PATHINFO_EXTENSION)), array("gzip", "gz", "zip"))) {
                    // gzip file and preserve it's base name
                    $gzFilePath = $currentUploadDir . '/' . basename($filePath) . '.gz';
                    $command = sprintf("gzip -c %s > %s", escapeshellarg($filePath), escapeshellarg($gzFilePath));
                    $process = ProcessPolyfill::createProcess($command);
                    $process->setTimeout(null);
                    if (0 !== $process->run()) {
                        $error = sprintf(
                            'The command "%s" failed.' . "\nExit Code: %s(%s)",
                            $process->getCommandLine(),
                            $process->getExitCode(),
                            $process->getExitCodeText()
                        );
                        throw new ClientException("Failed to gzip file. " . $error);
                    }
                    $slices[$key] = $gzFilePath;
                }
            }
            $newOptions->setFileName($newOptions->getFileName() . '.gz');
        }

        $fileSize = 0;
        foreach ($slices as $filePath) {
            if (!is_readable($filePath)) {
                throw new ClientException("File is not readable: " . $filePath, null, null, 'fileNotReadable');
            }
            $fileSize += filesize($filePath);
        }

        $newOptions
            ->setSizeBytes($fileSize)
            ->setFederationToken(true)
            ->setIsSliced(true);

        // 1. prepare resource
        $prepareResult = $this->prepareFileUpload($newOptions);

        switch ($prepareResult['provider']) {
            case self::FILE_PROVIDER_AZURE:
                $this->uploadSlicedFileToAbs($prepareResult, $slices);
                break;
            case self::FILE_PROVIDER_AWS:
                $this->uploadSlicedFileToS3($prepareResult, $slices, $options, $transferOptions);
                break;
        }

        // Cleanup
        if ($fs) {
            $fs->remove($currentUploadDir);
        }

        return $prepareResult['id'];
    }

    private function uploadSlicedFileToAbs(
        array $prepareResult,
        array $slices
    ) {
        $blobClient = BlobClientFactory::createClientFromConnectionString(
            $prepareResult['absUploadParams']['absCredentials']['SASConnectionString']
        );

        $uploader = new ABSUploader($blobClient);
        $uploader->uploadSlicedFile(
            $prepareResult['absUploadParams']['container'],
            $prepareResult['absUploadParams']['blobName'],
            $slices
        );

        $manifest = [
            'entries' => [],
        ];

        foreach ($slices as $filePath) {
            $manifest['entries'][] = [
                'url' => sprintf(
                    'azure://%s.blob.core.windows.net/%s/%s%s',
                    $prepareResult['absUploadParams']['accountName'],
                    $prepareResult['absUploadParams']['container'],
                    $prepareResult['absUploadParams']['blobName'],
                    basename($filePath)
                ),
            ];
        }
        $blobClient->createBlockBlob($prepareResult['absUploadParams']['container'], $prepareResult['absUploadParams']['blobName'] . 'manifest', json_encode($manifest));
    }

    private function uploadSlicedFileToS3(
        array $preparedFileResult,
        array $slices,
        FileUploadOptions $newOptions,
        FileUploadTransferOptions $transferOptions = null
    ) {
        $uploadParams = $preparedFileResult['uploadParams'];
        $options = [
            'version' => '2006-03-01',
            'retries' => $this->getAwsRetries(),
            'region' => $preparedFileResult['region'],
            'debug' => false,
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ]
        ];

        if ($this->isAwsDebug()) {
            $logfn = function ($message) {
                if (trim($message) != '') {
                    $this->log($message, ['source' => 'AWS SDK PHP debug']);
                }
            };
            $options['debug'] = [
                'logfn' => function ($message) use ($logfn) {
                    call_user_func($logfn, $message);
                },
                'stream_size' => 0,
                'scrub_auth' => true,
                'http' => true
            ];
        }

        $s3Client = new S3Client($options);
        $s3Uploader = new S3Uploader($s3Client, $transferOptions);
        $s3Uploader->uploadSlicedFile(
            $uploadParams['bucket'],
            $uploadParams['key'],
            $uploadParams['acl'],
            $slices,
            $newOptions->getIsEncrypted() ? $uploadParams['x-amz-server-side-encryption'] : null
        );
        // Upload manifest
        $manifest = [
            'entries' => [],
        ];
        foreach ($slices as $filePath) {
            $manifest['entries'][] = [
                "url" => "s3://" . $uploadParams['bucket'] . "/" . $uploadParams['key'] . basename($filePath),
            ];
        }
        $manifestUploadOptions = [
            'Bucket' => $uploadParams['bucket'],
            'Key' => $uploadParams['key'] . 'manifest',
            'Body' => json_encode($manifest),
        ];
        if ($newOptions->getIsEncrypted()) {
            $manifestUploadOptions['ServerSideEncryption'] = $uploadParams['x-amz-server-side-encryption'];
        }
        $s3Client->putObject($manifestUploadOptions);
    }

    public function downloadFile($fileId, $destination, GetFileOptions $getOptions = null)
    {
        $getOptions = ($getOptions)? $getOptions : new GetFileOptions();
        $getOptions->setFederationToken(true);
        $fileInfo = $this->getFile($fileId, $getOptions);
        switch ($fileInfo['provider']) {
            case self::FILE_PROVIDER_AZURE:
                $this->downloadAbsFile($fileInfo, $destination);
                break;
            case self::FILE_PROVIDER_AWS:
                $this->downloadS3File($fileInfo, $destination);
                break;
        }
    }

    private function downloadAbsFile(array $fileInfo, $destination)
    {
        $blobClient = BlobClientFactory::createClientFromConnectionString(
            $fileInfo['absCredentials']['SASConnectionString']
        );
        $getResult = $blobClient->getBlob($fileInfo['absPath']['container'], $fileInfo['absPath']['name']);
        (new Filesystem())->dumpFile($destination, $getResult->getContentStream());
    }

    private function downloadS3File(array $fileInfo, $destination)
    {
        $s3Client = new S3Client([
            'version' => 'latest',
            'region' => $fileInfo['region'],
            'credentials' => [
                'key' => $fileInfo['credentials']['AccessKeyId'],
                'secret' => $fileInfo['credentials']['SecretAccessKey'],
                'token' => $fileInfo['credentials']['SessionToken'],
            ],
        ]);
        $s3Client->getObject([
            'Bucket' => $fileInfo['s3Path']['bucket'],
            'Key' => $fileInfo['s3Path']['key'],
            'SaveAs' => $destination
        ]);
    }

    public function downloadSlicedFile($fileId, $destinationFolder)
    {
        $fileInfo = $this->getFile($fileId, (new GetFileOptions())->setFederationToken(true));
        switch ($fileInfo['provider']) {
            case self::FILE_PROVIDER_AZURE:
                return $this->downloadAbsSlicedFile($fileInfo, $destinationFolder);
            case self::FILE_PROVIDER_AWS:
                return $this->downloadS3SlicedFile($fileInfo, $destinationFolder);
        }
    }

    private function downloadAbsSlicedFile(array $fileInfo, $destinationFolder)
    {
        $blobClient = BlobClientFactory::createClientFromConnectionString(
            $fileInfo['absCredentials']['SASConnectionString']
        );
        if (!file_exists($destinationFolder)) {
            $fs = new Filesystem();
            $fs->mkdir($destinationFolder);
        }

        if (substr($destinationFolder, -1) !== '/') {
            $destinationFolder .= '/';
        }

        $getResult = $blobClient->getblob($fileInfo['absPath']['container'], $fileInfo['absPath']['name'] . 'manifest');
        $manifest = \GuzzleHttp\json_decode(stream_get_contents($getResult->getContentStream()), true);
        $slices = [];
        $fs = new Filesystem();
        foreach ($manifest['entries'] as $entry) {
            $blobPath = explode(sprintf(
                'blob.core.windows.net/%s/',
                $fileInfo['absPath']['container']
            ), $entry['url'])[1];
            $getResult = $blobClient->getBlob($fileInfo['absPath']['container'], $blobPath);
            $slices[] = $destinationFile = $destinationFolder . basename($entry['url']);
            $fs->dumpFile($destinationFile, $getResult->getContentStream());
        }
        return $slices;
    }

    private function downloadS3SlicedFile(array $fileInfo, $destinationFolder)
    {
        $s3Client = new S3Client([
            'version' => 'latest',
            'region' => $fileInfo['region'],
            'credentials' => [
                'key' => $fileInfo['credentials']['AccessKeyId'],
                'secret' => $fileInfo['credentials']['SecretAccessKey'],
                'token' => $fileInfo['credentials']['SessionToken'],
            ],
        ]);

        if (!file_exists($destinationFolder)) {
            $fs = new Filesystem();
            $fs->mkdir($destinationFolder);
        }

        if (substr($destinationFolder, -1) !== '/') {
            $destinationFolder .= '/';
        }

        $object = $s3Client->getObject([
            'Bucket' => $fileInfo['s3Path']['bucket'],
            'Key' => $fileInfo['s3Path']['key'] . 'manifest',
        ]);
        $manifest = \GuzzleHttp\json_decode($object['Body'], true);
        $slices = [];
        foreach ($manifest['entries'] as $entry) {
            $object = $s3Client->getObject([
                'Bucket' => $fileInfo['s3Path']['bucket'],
                'Key' => strtr($entry['url'], ['s3://' . $fileInfo['s3Path']['bucket'] . '/' => '']),
            ]);
            $slices[] = $destinationFile = $destinationFolder . basename($entry['url']);
            file_put_contents($destinationFile, $object['Body']);
        }
        return $slices;
    }

    /**
     * Prepares file metadata in Storage
     * http://docs.keboola.apiary.io/#post-%2Fv2%2Fstorage%2Ffiles%2Fprepare
     *
     * @param FileUploadOptions $options
     * @return array file info
     */
    public function prepareFileUpload(FileUploadOptions $options)
    {
        return $this->apiPost("files/prepare", array(
            'isPublic' => $options->getIsPublic(),
            'isPermanent' => $options->getIsPermanent(),
            'isEncrypted' => $options->getIsEncrypted(),
            'isSliced' => $options->getIsSliced(),
            'notify' => $options->getNotify(),
            'name' => $options->getFileName(),
            'sizeBytes' => $options->getSizeBytes(),
            'tags' => $options->getTags(),
            'federationToken' => $options->getFederationToken(),
        ));
    }

    /**
     * Delete a single file
     * @param $fileId
     * @return mixed|string
     */
    public function deleteFile($fileId)
    {
        return $this->apiDelete("files/$fileId");
    }


    /**
     * Get a single file
     * @param string $fileId
     * @return array
     */
    public function getFile($fileId, GetFileOptions $options = null)
    {
        if (empty($fileId)) {
            throw new ClientException('File id cannot be empty');
        }
        return $this->apiGet("files/$fileId?" . http_build_query($options ? $options->toArray() : array()));
    }

    /**
     * Delete file tag
     * @param $fileId
     * @param $tagName
     */
    public function deleteFileTag($fileId, $tagName)
    {
        $this->apiDelete("files/$fileId/tags/$tagName");
    }

    public function addFileTag($fileId, $tagName)
    {
        $this->apiPost("files/$fileId/tags", array(
            'tag' => $tagName,
        ));
    }

    /**
     * List files
     *
     * @param ListFilesOptions $options
     * @return array
     */
    public function listFiles(ListFilesOptions $options = null)
    {
        return $this->apiGet('files?' . http_build_query($options ? $options->toArray() : array()));
    }


    /**
     * Create new event
     *
     * @param Event $event
     * @return int - created event id
     */
    public function createEvent(Event $event)
    {
        $result = $this->apiPost('events', array(
            'component' => $event->getComponent(),
            'configurationId' => $event->getConfigurationId(),
            'runId' => $event->getRunId(),
            'message' => $event->getMessage(),
            'description' => $event->getDescription(),
            'type' => $event->getType(),
            'params' => json_encode($event->getParams()),
            'results' => json_encode($event->getResults()),
            'duration' => $event->getDuration(),
        ));
        return $result['id'];
    }

    /**
     * @param $id
     * @return array
     */
    public function getEvent($id)
    {
        return $this->apiGet('events/' . $id);
    }

    /**
     * @param array $params
     * @return array
     */
    public function listEvents($params = array())
    {
        $defaultParams = array(
            'limit' => 100,
            'offset' => 0,
        );

        if (!is_array($params)) {
            // BC compatibility
            $args = func_get_args();
            $params = array(
                'limit' => $args[0],
            );
            if (isset($args[1])) {
                $params['offset'] = $args[1];
            }
        }

        $queryParams = array_merge($defaultParams, $params);
        return $this->apiGet('events?' . http_build_query($queryParams));
    }

    /**
     * @param $tableId
     * @param array $params
     * @return array
     */
    public function listTableEvents($tableId, $params = [])
    {
        $defaultParams = array(
            'limit' => 100,
            'offset' => 0,
        );

        $queryParams = array_merge($defaultParams, $params);
        return $this->apiGet("tables/{$tableId}/events?" . http_build_query($queryParams));
    }

    /**
     * @param $bucketId
     * @param array $params
     * @return mixed|string
     */
    public function listBucketEvents($bucketId, $params = [])
    {
        $defaultParams = array(
            'limit' => 100,
            'offset' => 0,
        );

        $queryParams = array_merge($defaultParams, $params);
        return $this->apiGet("buckets/{$bucketId}/events?" . http_build_query($queryParams));
    }

    /**
     * @param int $tokenId
     * @param array $params
     * @return array
     */
    public function listTokenEvents($tokenId, $params = array())
    {
        $defaultParams = array(
            'limit' => 100,
            'offset' => 0,
        );

        $queryParams = array_merge($defaultParams, $params);
        return $this->apiGet("tokens/{$tokenId}/events?" . http_build_query($queryParams));
    }

    /**
     * @param $id
     * @return array
     */
    public function getSnapshot($id)
    {
        return $this->apiGet("snapshots/$id");
    }

    /**
     * @param $id
     */
    public function deleteSnapshot($id)
    {
        $result = $this->apiDelete("snapshots/$id");
        $this->log("Snapshot $id deleted");
    }

    /**
     * Unique 64bit sequence generator
     * @return int generated id
     */
    public function generateId()
    {
        $result = $this->apiPost('tickets');
        return $result['id'];
    }

    /**
     * @param null $previousRunId Allows runId hierarchy. If previous run Id is set, returned id will be in form of
     * previousRunId.newRunId
     *
     * @return string
     */
    public function generateRunId($previousRunId = null)
    {
        $newRunId = $this->generateId();

        if ($previousRunId) {
            return $previousRunId . '.' . $newRunId;
        } else {
            return $newRunId;
        }
    }

    /**
     *
     * Prepare URL and call a GET request
     *
     * @param string $url
     * @param string null $fileName
     * @return mixed|string
     */
    public function apiGet($url, $fileName = null)
    {
        return $this->request('GET', $url, array(), $fileName);
    }

    /**
     *
     * Prepare URL and call a POST request
     *
     * @param string $url
     * @param array $postData
     * @return mixed|string
     */
    public function apiPost($url, $postData = null, $handleAsyncTask = true)
    {
        return $this->request('post', $url, array('form_params' => $postData), null, $handleAsyncTask);
    }

    public function apiPostMultipart($url, $postData = null, $handleAsyncTask = true)
    {
        return $this->request('post', $url, array('multipart' => $postData), null, $handleAsyncTask);
    }

    private function apiPostJson($url, $data = [])
    {
        return $this->request('POST', $url, [
            'json' => $data,
        ]);
    }

    /**
     *
     * Prepare URL and call a POST request
     *
     * @param string $url
     * @param array $postData
     * @return mixed|stringgit d
     */
    public function apiPut($url, $postData = null)
    {
        return $this->request('put', $url, [
            'form_params' => $postData,
        ]);
    }

    /**
     *
     * Prepare URL and call a DELETE request
     *
     * @param string $url
     * @return mixed|string
     */
    public function apiDelete($url)
    {
        return $this->request('delete', $url);
    }

    public function apiDeleteParams($url, $data)
    {
        $options = array();
        $options['headers']['Content-Type'] = 'application/x-www-form-urlencoded';
        $options['body'] = http_build_query($data, '', '&');
        return $this->request('delete', $url, $options);
    }

    protected function request($method, $url, $options = array(), $responseFileName = null, $handleAsyncTask = true)
    {
        $url = self::API_VERSION . "/storage/" . $url;
        $requestOptions = array_merge($options, [
            'timeout' => $this->getTimeout(),
        ]);

        if ($responseFileName !== null) {
            $requestOptions['stream'] = true;
        }

        $defaultHeaders = [
            'X-StorageApi-Token' => $this->token,
            'Accept-Encoding' => 'gzip',
            'User-Agent' => $this->getUserAgent(),
        ];
        if (isset($options['headers'])) {
            $requestOptions['headers'] = array_merge($options['headers'], $defaultHeaders);
        } else {
            $requestOptions['headers'] = $defaultHeaders;
        }

        if ($this->getRunId()) {
            $requestOptions['headers']['X-KBC-RunId'] = $this->getRunId();
        }

        try {
            /**
             * @var ResponseInterface $response
             */
            $response = $this->client->request($method, $url, $requestOptions);
        } catch (RequestException $e) {
            $response = $e->getResponse();
            $body = $response ? json_decode((string)$response->getBody(), true) : array();

            if ($response && $response->getStatusCode() == 503) {
                throw new MaintenanceException(isset($body["reason"]) ? $body['reason'] : 'Maintenance', $response && $response->hasHeader('Retry-After') ? (string)$response->getHeader('Retry-After')[0] : null, $body);
            }

            throw new ClientException(
                isset($body['error']) ? $body['error'] : $e->getMessage(),
                $response ? $response->getStatusCode() : $e->getCode(),
                $e,
                isset($body['code']) ? $body['code'] : "",
                $body
            );
        }

        // wait for asynchronous task completion
        if ($handleAsyncTask && $response->getStatusCode() == 202) {
            return $this->handleAsyncTask($response);
        }

        if ($responseFileName) {
            $responseFile = fopen($responseFileName, "w");
            if (!$responseFile) {
                throw new ClientException("Cannot open file {$responseFileName}");
            }
            $body = $response->getBody();
            while (!$body->eof()) {
                fwrite($responseFile, $body->read(1024 * 10));
            }
            fclose($responseFile);
            return "";
        }

        if ($response->hasHeader('Content-Type') && $response->getHeader('Content-Type')[0] == 'application/json') {
            return json_decode((string)$response->getBody(), true);
        }

        return (string)$response->getBody();
    }

    private function fixRequestBody(array $body)
    {
        $fixedBody = array();
        foreach ($body as $key => $value) {
            if (!is_array($value)) {
                $fixedBody[$key] = $value;
                continue;
            }

            foreach ($value as $deeperKey => $deeperValue) {
                $fixedBody[sprintf("%s[%s]", $key, $deeperKey)] = $deeperValue;
            }
        }
        return $fixedBody;
    }

    /**
     * @param Response $jobCreatedResponse
     * @return mixed
     * @throws ClientException
     */
    private function handleAsyncTask(Response $jobCreatedResponse)
    {
        $job = json_decode((string)$jobCreatedResponse->getBody(), true);
        $job = $this->waitForJob($job['id']);
        $this->handleJobError($job);
        return $job['results'];
    }

    /**
     * @param array $job
     * @throws ClientException
     */
    private function handleJobError($job)
    {
        if ($job['status'] == 'error') {
            throw new ClientException(
                $job['error']['message'],
                null,
                null,
                $job['error']['code'],
                array_merge($job['error'], [
                    'job' => $job,
                ])
            );
        }
    }

    /**
     * @param $jobId
     * @return array|null
     */
    public function waitForJob($jobId)
    {
        $retries = 0;
        $job = null;

        // poll for status
        do {
            if ($retries > 0) {
                $waitSeconds = call_user_func($this->jobPollRetryDelay, $retries);
                sleep($waitSeconds);
            }
            $retries++;

            $job = $this->getJob($jobId);
            $jobId = $job['id'];
        } while (!in_array($job['status'], array('success', 'error')));

        return $job;
    }

    /**
     * @param array $jobIds
     * @return array
     * @throws ClientException
     */
    public function handleAsyncTasks($jobIds)
    {
        $jobResults = [];
        foreach ($jobIds as $jobId) {
            $jobResult = $this->waitForJob($jobId);
            $this->handleJobError($jobResult);
            $jobResults[] = $jobResult;
        }
        return $jobResults;
    }


    /**
     * @param string $message Message to log
     * @param array $context Data to log
     *
     */
    private function log($message, $context = array())
    {
        $this->logger->debug($message, $context);
    }

    /**
     * @param LoggerInterface $logger
     */
    private function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     *
     * Parse CSV string into array
     * uses str_getcsv function
     *
     * @static
     * @param string $csvString
     * @param bool $header if first line contains header
     * @param string $delimiter CSV delimiter
     * @param string $enclosure CSV field enclosure (should remain '"' with new CSV handling)
     * @return array
     */
    public static function parseCsv($csvString, $header = true, $delimiter = ",", $enclosure = '"')
    {
        $data = array();
        $headers = array();
        $firstLine = true;

        $tmpFile = tmpfile();
        fwrite($tmpFile, $csvString);
        rewind($tmpFile);

        if (!$enclosure) {
            $enclosure = chr(0);
        }

        while ($parsedLine = fgetcsv($tmpFile, null, $delimiter, $enclosure, '"')) {
            if (!$header) {
                $data[] = $parsedLine;
            } else {
                if ($firstLine) {
                    $headers = $parsedLine;
                } else {
                    $lineData = array();
                    foreach ($headers as $i => $headerName) {
                        $lineData[$headerName] = $parsedLine[$i];
                    }
                    $data[] = $lineData;
                }
            }
            if ($firstLine) {
                $firstLine = false;
            }
        }
        fclose($tmpFile);

        return $data;
    }

    /**
     *
     * Set CURL timeout in seconds
     *
     * @param integer $timeout
     */
    public function setTimeout($timeout)
    {
        $this->connectionTimeout = $timeout;
    }

    /**
     *
     * Get CURL timeout in seconds
     *
     * @return int
     */
    public function getTimeout()
    {
        return $this->connectionTimeout;
    }

    public function getRunId()
    {
        return $this->runId;
    }

    /**
     * @param $runId
     * @return Client
     */
    public function setRunId($runId)
    {
        $this->runId = $runId;
        return $this;
    }

    /**
     * @return int
     */
    public function getBackoffMaxTries()
    {
        return $this->backoffMaxTries;
    }

    /**
     *
     * Returns components from indexAction
     * @deprecated
     *
     * @return array
     */
    public function getComponents()
    {
        $data = $this->indexAction();
        $components = array();
        if (!isset($data["components"])) {
            return $components;
        }
        foreach ($data["components"] as $component) {
            $components[$component["id"]] = $component["uri"];
        }
        return $components;
    }

    public function getStats(StatsOptions $options)
    {
        return $this->apiGet('stats?' . http_build_query($options->toArray()));
    }

    private function prepareMultipartData($data)
    {
        $multipart = [];
        foreach ($data as $key => $value) {
            $multipart[] = [
                'name' => $key,
                'contents' => in_array(gettype($value), ['object', 'resource', 'NULL']) ? $value : (string)$value,
            ];
        }
        return $multipart;
    }

    /**
     * Remove table primary key
     *
     * @param $tableId
     */
    public function removeTablePrimaryKey($tableId)
    {
        $this->apiDelete("tables/$tableId/primary-key");
        $this->log("Table $tableId primary key deleted");
    }

    /**
     * Create table primary key
     *
     * @param string $tableId
     * @param array $columns
     */
    public function createTablePrimaryKey($tableId, $columns)
    {
        $data = array(
            'columns' => $columns,
        );
        $this->apiPost("tables/$tableId/primary-key", $data);
    }

    public function createTrigger($option)
    {
        return $this->apiPost("triggers/", $option);
    }

    public function updateTrigger($triggerId, $options)
    {
        return $this->apiPut('triggers/' . $triggerId .'/', $options);
    }

    public function getTrigger($triggerId)
    {
        return $this->apiGet('triggers/' . $triggerId .'/');
    }

    public function deleteTrigger($triggerId)
    {
        return $this->apiDelete('triggers/' . $triggerId .'/');
    }

    public function listTriggers($filter = [])
    {
        return $this->apiGet('triggers/?' . http_build_query($filter));
    }

    /**
     * @return int
     */
    public function getAwsRetries()
    {
        return $this->awsRetries;
    }

    /**
     * @return boolean
     */
    public function isAwsDebug()
    {
        return $this->awsDebug;
    }
}
