<?php
namespace Keboola\StorageApi;

use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use Google\Auth\FetchAuthTokenInterface;
use Google\Cloud\Core\Exception\NotFoundException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use GuzzleHttp\Utils;
use Keboola\StorageApi\Client\RequestTimeoutMiddleware;
use Keboola\StorageApi\Downloader\BlobClientFactory;
use Keboola\StorageApi\Options\BackendConfiguration;
use Keboola\StorageApi\Options\BucketUpdateOptions;
use Keboola\StorageApi\Options\Components\SearchComponentConfigurationsOptions;
use Keboola\StorageApi\Options\FileUploadTransferOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\IndexOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\Options\SearchTablesOptions;
use Keboola\StorageApi\Options\StatsOptions;
use Keboola\StorageApi\Options\TableWithConfigurationOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use MicrosoftAzure\Storage\Blob\Models\CommitBlobBlocksOptions;
use MicrosoftAzure\Storage\Blob\Models\CreateBlockBlobOptions;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Options\FileUploadOptions;
use Google\Cloud\Storage\StorageClient as GoogleStorageClient;

class Client
{
    // Request options
    const ALLOWED_REQUEST_OPTIONS = [Client::REQUEST_OPTION_EXTENDED_TIMEOUT];
    const REQUEST_OPTION_EXTENDED_TIMEOUT = 'isExtendedTimeout';

    // Stage names
    const DEFAULT_RETRIES_COUNT = 15;
    const STAGE_IN = 'in';
    const STAGE_OUT = 'out';
    const STAGE_SYS = 'sys';
    const API_VERSION = 'v2';

    const VERSION = '14';

    const FILE_PROVIDER_AWS = 'aws';
    const FILE_PROVIDER_AZURE = 'azure';
    const FILE_PROVIDER_GCP = 'gcp';

    // Errors
    const ERROR_CANNOT_DOWNLOAD_FILE = 'Cannot download file "%s" (ID %s) from Storage, please verify the contents of the file and that the file has not expired.';

    // Token string
    public $token;

    // current run id sent with all request
    private $runId = null;

    // configuration will be send with all requests
    private ?BackendConfiguration $backendConfiguration = null;

    // API URL
    private $apiUrl;

    private $backoffMaxTries = 11;

    private $awsRetries = self::DEFAULT_RETRIES_COUNT;

    private $awsDebug = false;

    // User agent header send with each API request
    private $userAgent = 'Keboola Storage API PHP Client';

    /**
     * @var callable
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
     *     - handler: custom Guzzle handler, allows mocking responses in tests
     */
    public function __construct(array $config = [])
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
            $this->backoffMaxTries = (int) $config['backoffMaxTries'];
        }

        if (isset($config['awsRetries'])) {
            $this->awsRetries = (int) $config['awsRetries'];
        }

        if (isset($config['awsDebug'])) {
            $this->awsDebug = (bool) $config['awsDebug'];
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

        $this->initClient($config);
        $this->tokens = new Tokens($this);
    }

    private function initClient(array $config)
    {
        $handlerStack = HandlerStack::create([
            'backoffMaxTries' => $this->backoffMaxTries,
            'handler' => $config['handler'] ?? null,
        ]);

        $handlerStack->push((RequestTimeoutMiddleware::factory()));
        $handlerStack->push(Middleware::log(
            $this->logger,
            new MessageFormatter('{hostname} {req_header_User-Agent} - [{ts}] "{method} {resource} {protocol}/{version}" {code} {res_header_Content-Length}'),
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
        $options = new IndexOptions();
        $options->setExclude(['components']);
        $indexResult = $this->indexAction($options);

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
    public function listBuckets($options = [])
    {
        return $this->apiGet('buckets?' . http_build_query($options));
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
            if ($bucket['stage'] == $stage && $bucket['name'] == $name) {
                return $bucket['id'];
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
        return $this->apiGet('buckets/' . $bucketId);
    }

    /**
     * @param BucketUpdateOptions $options
     * @return array
     */
    public function updateBucket(BucketUpdateOptions $options)
    {
        return $this->apiPutJson('buckets/' . $options->getBucketId(), $options->toParamsArray());
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
    public function createBucket($name, $stage, $description = '', $backend = null, $displayName = null)
    {
        $options = [
            'name' => $name,
            'stage' => $stage,
            'description' => $description,
        ];

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

        /** @var array{id: string} $result */
        $result = $this->apiPostJson('buckets', $options);

        $this->log("Bucket {$result["id"]} created", ['options' => $options, 'result' => $result]);

        return $result['id'];
    }

    /**
     * @param string[] $path
     */
    public function registerBucket(
        string $name,
        array $path,
        string $stage,
        string $description = '',
        ?string $backend = null,
        ?string $displayName = null
    ): string {
        $data = [
            'name' => $name,
            'path' => $path,
            'stage' => $stage,
            'description' => $description,
        ];

        if ($backend !== null) {
            $data['backend'] = $backend;
        }

        if ($displayName !== null) {
            $data['displayName'] = $displayName;
        }

        $result = $this->apiPostJson('buckets/register', $data);

        $this->log("Bucket {$result["id"]} registered", ['options' => $data, 'result' => $result]);

        return $result['id'];
    }

    /**
     * @return mixed
     */
    public function refreshBucket(string $bucketId)
    {
        $url = 'buckets/' . $bucketId . '/refresh';

        return $this->apiPutJson($url);
    }

    /**
     * Link shared bucket to project
     *
     * @param string $name new bucket name
     * @param string $stage bucket stage
     * @param int $sourceProjectId
     * @param string $sourceBucketId
     * @param string|null $displayName bucket display name
     * @return string
     */
    public function linkBucket($name, $stage, $sourceProjectId, $sourceBucketId, $displayName = null, $async = false)
    {
        $options = [
            'name' => $name,
            'stage' => $stage,
            'sourceProjectId' => $sourceProjectId,
            'sourceBucketId' => $sourceBucketId,
        ];

        if ($displayName) {
            $options['displayName'] = $displayName;
        }

        $url = 'buckets';
        if ($async) {
            $url .= '?' . http_build_query(['async' => $async]);
        }

        /** @var array{id: string} $result */
        $result = $this->apiPostJson($url, $options, $async);

        $this->log("Shared bucket {$result["id"]} linked to the project", ['options' => $options, 'result' => $result]);

        return $result['id'];
    }

    /**
     *
     * Delete a bucket. Only empty buckets can be deleted
     *
     * @param string $bucketId
     * @param array $options - (bool) force
     * @return mixed|string
     */
    public function dropBucket($bucketId, $options = [])
    {
        $url = 'buckets/' . $bucketId;

        $allowedOptions = [
            'force',
            'async',
        ];

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        $url .= '?' . http_build_query($filteredOptions);

        return $this->apiDelete($url);
    }

    /**
     * @param string $bucketId
     * @param array $options
     * @return mixed
     * @deprecated use shareBucketToUsers, shareBucketToProjects, shareOrganizationProjectBucket or
     *     shareOrganizationBucket instead
     */
    public function shareBucket($bucketId, $options = [])
    {
        $url = 'buckets/' . $bucketId . '/share';

        $isAsync = false;
        if (array_key_exists('async', $options)) {
            $isAsync = $options['async'];
        }

        $url .= '?' . http_build_query($options);

        // keep request with form-data because this endpoint deprecated
        $result = $this->apiPost($url, [], $isAsync);

        $this->log("Bucket {$bucketId} shared", ['result' => $result]);

        return $result;
    }

    public function shareOrganizationBucket($bucketId, $async = false)
    {
        $url = 'buckets/' . $bucketId . '/share-organization';

        if ($async) {
            $url .= '?' . http_build_query(['async' => $async]);
        }

        $result = $this->apiPostJson($url, [], $async);

        $this->log("Bucket {$bucketId} shared", ['result' => $result]);

        return $result;
    }

    public function shareOrganizationProjectBucket($bucketId, $async = false)
    {
        $url = 'buckets/' . $bucketId . '/share-organization-project';

        if ($async) {
            $url .= '?' . http_build_query(['async' => $async]);
        }

        $result = $this->apiPostJson($url, [], $async);

        $this->log("Bucket {$bucketId} shared", ['result' => $result]);

        return $result;
    }

    /**
     * @param array $targetProjectIds
     * @param bool $async
     * @return array{0: array, 1: array}
     */
    private function shareBucketToProjectsPrepareOptions($targetProjectIds, $async): array
    {
        $query = [];
        if ($async) {
            $query['async'] = $async;
        }

        $data = [
            'targetProjectIds' => $targetProjectIds,
        ];

        return [
            $query,
            $data,
        ];
    }

    /**
     * @param string $bucketId
     * @param array $targetProjectIds
     * @param bool $async
     * @return array
     */
    public function shareBucketToProjects($bucketId, $targetProjectIds, $async = false)
    {
        [$query, $data] = $this->shareBucketToProjectsPrepareOptions($targetProjectIds, $async);

        $url = sprintf('buckets/%s/share-to-projects', $bucketId);
        if ($query) {
            $url .= '?' . http_build_query($query);
        }

        $result = $this->apiPostJson($url, $data, $async);
        assert(is_array($result));

        $this->log("Bucket {$bucketId} shared", ['result' => $result]);

        return $result;
    }

    /**
     * @deprecated use self::shareBucketToProjects instead
     * @param string $bucketId
     * @param array $targetProjectIds
     * @param bool $async
     * @return array
     */
    public function shareBucketToProjectsAsQuery($bucketId, $targetProjectIds, $async = false)
    {
        [$query, $data] = $this->shareBucketToProjectsPrepareOptions($targetProjectIds, $async);

        $query = array_merge($query, $data);

        $url = sprintf('buckets/%s/share-to-projects', $bucketId);
        if ($query) {
            $url .= '?' . http_build_query($query);
        }

        $result = $this->apiPost($url, [], $async);
        assert(is_array($result));

        $this->log("Bucket {$bucketId} shared", ['result' => $result]);

        return $result;
    }

    /**
     * @param array $targetUsers
     * @param bool $async
     * @return array{0: array, 1: array}
     */
    private function shareBucketToUsersPrepareOptions($targetUsers, $async = false): array
    {
        $query = [];
        if ($async) {
            $query['async'] = $async;
        }

        $data = [
            'targetUsers' => $targetUsers,
        ];

        return [
            $query,
            $data,
        ];
    }

    /**
     * @param string $bucketId
     * @param array $targetUsers
     * @param bool $async
     * @return array
     */
    public function shareBucketToUsers($bucketId, $targetUsers = [], $async = false)
    {
        [$query, $data] = $this->shareBucketToUsersPrepareOptions($targetUsers, $async);

        $url = sprintf('buckets/%s/share-to-users', $bucketId);
        if ($query) {
            $url .= '?' . http_build_query($query);
        }

        $result = $this->apiPostJson($url, $data, $async);
        assert(is_array($result));

        $this->log("Bucket {$bucketId} shared", ['result' => $result]);

        return $result;
    }

    /**
     * @deprecated use self::shareBucketToUsers instead
     * @param string $bucketId
     * @param array $targetUsers
     * @param bool $async
     * @return array
     */
    public function shareBucketToUsersAsQuery($bucketId, $targetUsers = [], $async = false)
    {
        [$query, $data] = $this->shareBucketToUsersPrepareOptions($targetUsers, $async);

        $query = array_merge($query, $data);

        $url = sprintf('buckets/%s/share-to-users', $bucketId);
        if ($query) {
            $url .= '?' . http_build_query($query);
        }

        $result = $this->apiPost($url, [], $async);
        assert(is_array($result));

        $this->log("Bucket {$bucketId} shared", ['result' => $result]);

        return $result;
    }

    public function changeBucketSharing($bucketId, $sharing, $async = false)
    {
        $url = 'buckets/' . $bucketId . '/share';

        $data = [
            'sharing' => $sharing,
        ];

        if ($async) {
            $url .= '?' . http_build_query(['async' => $async]);
        }

        $result = $this->apiPutJson($url, $data);

        $this->log("Bucket {$bucketId} sharing changed to {$sharing}", ['result' => $result]);

        return $result;
    }

    public function unshareBucket($bucketId, $async = false)
    {
        $url = 'buckets/' . $bucketId . '/share';

        if ($async) {
            $url .= '?' . http_build_query(['async' => $async]);
        }

        return $this->apiDelete($url);
    }

    public function forceUnlinkBucket($bucketId, $projectId, $options = [])
    {

        $url = 'buckets/' . $bucketId . '/links/' . $projectId;

        $allowedOptions = [
            'async',
        ];

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        $url .= '?' . http_build_query($filteredOptions);

        return $this->apiDelete($url);
    }

    public function isSharedBucket($bucketId)
    {
        $url = 'buckets/' . $bucketId;

        $result = $this->apiGet($url);

        return !empty($result['sharing']);
    }

    public function listSharedBuckets($options = [])
    {
        $url = 'shared-buckets';

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
     * @deprecated Use createTableAsync
     * @param $bucketId
     * @param $name
     * @param CsvFile $csvFile
     * @param array $options
     *  - primaryKey - string, multiple column primary keys separate by comma
     * @return string - created table id
     */
    public function createTable($bucketId, $name, CsvFile $csvFile, $options = [])
    {
        $options = [
            'bucketId' => $bucketId,
            'name' => $name,
            'delimiter' => $csvFile->getDelimiter(),
            'enclosure' => $csvFile->getEnclosure(),
            'escapedBy' => $csvFile->getEscapedBy(),
            'primaryKey' => isset($options['primaryKey']) ? $options['primaryKey'] : null,
            'columns' => isset($options['columns']) ? $options['columns'] : null,
            'data' => fopen($csvFile->getPathname(), 'r'),
            'syntheticPrimaryKeyEnabled' => isset($options['syntheticPrimaryKeyEnabled']) ? $options['syntheticPrimaryKeyEnabled'] : null,
        ];

        $tableId = $this->getTableId($name, $bucketId);
        if ($tableId) {
            return $tableId;
        }
        $result = $this->apiPostMultipart('buckets/' . $bucketId . '/tables', $this->prepareMultipartData($options));
        assert(is_array($result));

        $this->log("Table {$result['id']} created", ['options' => $options, 'result' => $result]);

        if (!empty($options['data']) && is_resource($options['data'])) {
            fclose($options['data']);
        }
        return $result['id'];
    }

    /**
     * Creates table with header of CSV file, then import whole csv file by async import
     * Handles async operation. Starts import job and waits when it is finished. Throws exception if job finishes with
     * error.
     *
     * Workflow:
     *  - Upload file to File Uploads
     *  - Initialize table import with previously uploaded file
     *  - Wait until job is finished
     *  - Return created table id
     *
     * @param string $bucketId
     * @param string $name
     * @param CsvFile $csvFile
     * @param array $options - see createTable method params
     * @return string - created table id
     */
    public function createTableAsync($bucketId, $name, CsvFile $csvFile, $options = [])
    {
        $options = [
            'bucketId' => $bucketId,
            'name' => $name,
            'delimiter' => $csvFile->getDelimiter(),
            'enclosure' => $csvFile->getEnclosure(),
            'escapedBy' => $csvFile->getEscapedBy(),
            'primaryKey' => isset($options['primaryKey']) ? $options['primaryKey'] : null,
            'distributionKey' => isset($options['distributionKey']) ? $options['distributionKey'] : null,
            'transactional' => isset($options['transactional']) ? $options['transactional'] : false,
            'columns' => isset($options['columns']) ? $options['columns'] : null,
            'syntheticPrimaryKeyEnabled' => isset($options['syntheticPrimaryKeyEnabled']) ? $options['syntheticPrimaryKeyEnabled'] : null,
        ];

        // upload file
        $fileId = $this->uploadFile(
            $csvFile->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['file-import'])
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
    public function createTableAsyncDirect($bucketId, $options = [])
    {
        $createdTable = $this->apiPostJson("buckets/{$bucketId}/tables-async", $options);
        return $createdTable['id'];
    }

    /**
     * Starts and waits for async creation of table definition
     *
     * @param $bucketId
     * @param array $data JSON table definition
     * @return string - created table id
     */
    public function createTableDefinition($bucketId, $data = [])
    {
        $createdTable = $this->apiPostJson("buckets/{$bucketId}/tables-definition", $data);
        return $createdTable['id'];
    }

    /**
     * Starts and waits for async creation of table from configuration
     */
    public function createTableWithConfiguration(string $bucketId, TableWithConfigurationOptions $data): string
    {
        $createdTable = $this->apiPostJson("buckets/{$bucketId}/tables-with-configuration", [
            'name' => $data->getTableName(),
            'configurationId' => $data->getConfigurationId(),
        ]);
        return $createdTable['id'];
    }

    /**
     * Starts and waits for async migration of table from configuration
     */
    public function migrateTableWithConfiguration(string $tableId): string
    {
        /** @var array{id: string} $migratedTable */
        $migratedTable = $this->apiPutJson('tables/' . $tableId . '/migrate');
        return $migratedTable['id'];
    }

    /**
     * @param string $bucketId destination bucket
     * @param string|int $snapshotId source snapshot
     * @param string|null $name table name (optional) otherwise fetched from snapshot
     * @return string - created table id
     */
    public function createTableFromSnapshot($bucketId, $snapshotId, $name = null)
    {
        return $this->createTableAsyncDirect($bucketId, [
            'snapshotId' => $snapshotId,
            'name' => $name,
        ]);
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
        return $this->createTableAsyncDirect($bucketId, [
            'sourceTableId' => $sourceTableId,
            'timestamp' => $timestamp,
            'name' => $name,
        ]);
    }

    /**
     * @param string $bucketId
     * @param string $sourceTableId
     * @param string|null $name
     * @param array $options
     *  - sourceTable
     *  - name (optional)
     *  - aliasFilter (optional)
     *  - (array) aliasColumns (optional)
     * @return string  - created table id
     */
    public function createAliasTable($bucketId, $sourceTableId, $name = null, $options = [])
    {
        $filteredOptions = [
            'sourceTable' => $sourceTableId,
            'name' => $name,
        ];

        if (isset($options['aliasFilter'])) {
            $filteredOptions['aliasFilter'] = (array) $options['aliasFilter'];
        }

        if (isset($options['aliasColumns'])) {
            $filteredOptions['aliasColumns'] = (array) $options['aliasColumns'];
        }

        /** @var array{id:string} $result */
        $result = $this->apiPostJson('buckets/' . $bucketId . '/table-aliases', $filteredOptions);
        $this->log("Table alias {$result["id"]}  created", ['options' => $filteredOptions, 'result' => $result]);
        return $result['id'];
    }

    /**
     * @param $tableId
     * @return int - snapshot id
     */
    public function createTableSnapshot($tableId, $snapshotDescription = null)
    {
        /** @var array{id: int} $result */
        $result = $this->apiPostJson("tables/{$tableId}/snapshots", [
            'description' => $snapshotDescription,
        ]);
        $this->log("Snapthos {$result['id']} of table {$tableId} created.");
        return $result['id'];
    }

    /**
     * @param string $tableId
     * @param array $options
     * @return string|null
     */
    public function updateTable($tableId, $options)
    {
        $allowedOptions = [
            'displayName',
            'async',
        ];
        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        $url = 'tables/' . $tableId;
        $async = $filteredOptions['async'] ?? null;
        if ($async !== null) {
            $url .= '?' . http_build_query(['async' => $async]);
            unset($filteredOptions['async']);
        }

        /** @var array{id: string} $result */
        $result = $this->apiPutJson($url, $filteredOptions);
        $this->log("Table {$tableId} updated");
        if ($async === true) {
            // async job has no result
            return null;
        }
        return $result['id'];
    }

    /**
     * @param $tableId
     * @return mixed|string
     */
    public function listTableSnapshots($tableId, $options = [])
    {
        return $this->apiGet("tables/{$tableId}/snapshots?" . http_build_query($options));
    }

    /**
     * @param $tableId
     * @param array $filter
     * @return array
     */
    public function setAliasTableFilter($tableId, array $filter)
    {
        $result = $this->apiPostJson("tables/$tableId/alias-filter", $filter);
        $this->log("Table $tableId  filter set", [
            'filter' => $filter,
            'result' => $result,
        ]);
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
        $this->apiPostJson("tables/{$tableId}/alias-columns-auto-sync");
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
    public function listTables($bucketId = null, $options = [])
    {
        if ($bucketId) {
            return $this->apiGet("buckets/{$bucketId}/tables?" . http_build_query($options));
        }
        return $this->apiGet('tables?' . http_build_query($options));
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
            if ($table['name'] == $name) {
                return $table['id'];
            }
        }
        return false;
    }

    /**
     * @deprecated use writeTableAsync
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
    public function writeTable($tableId, CsvFile $csvFile, $options = [])
    {
        $optionsExtended = $this->writeTableOptionsPrepare(array_merge($options, [
            'delimiter' => $csvFile->getDelimiter(),
            'enclosure' => $csvFile->getEnclosure(),
            'escapedBy' => $csvFile->getEscapedBy(),
        ]));

        $optionsExtended['data'] = @fopen($csvFile->getRealPath(), 'r');
        if ($optionsExtended['data'] === false) {
            throw new ClientException('Failed to open temporary data file ' . $csvFile->getRealPath(), null, null, 'fileNotReadable');
        }

        $result = $this->apiPostMultipart("tables/{$tableId}/import", $this->prepareMultipartData($optionsExtended));

        $this->log("Data written to table {$tableId}", ['options' => $optionsExtended, 'result' => $result]);
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
    public function writeTableAsync($tableId, CsvFile $csvFile, $options = [])
    {
        $optionsExtended = array_merge($options, [
            'delimiter' => $csvFile->getDelimiter(),
            'enclosure' => $csvFile->getEnclosure(),
            'escapedBy' => $csvFile->getEscapedBy(),
        ]);

        // upload file
        $fileId = $this->uploadFile(
            $csvFile->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['table-import'])
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
    public function writeTableAsyncDirect($tableId, $options = [])
    {
        // TODO use apiPostJson after endpoint is ready
        return $this->apiPost("tables/{$tableId}/import-async", $this->writeTableOptionsPrepare($options));
    }

    /**
     * @param string $bucketId
     * @param array $options
     * @return int
     */
    public function queueTableCreate($bucketId, $options = [])
    {
        $job = $this->apiPostJson("buckets/{$bucketId}/tables-async", $options, false);
        return $job['id'];
    }

    /**
     * @param $tableId
     * @param array $options
     * @return int
     */
    public function queueTableImport($tableId, $options = [])
    {
        // TODO use apiPostJson after endpoint is ready
        $job = $this->apiPost("tables/{$tableId}/import-async", $this->writeTableOptionsPrepare($options), false);
        return $job['id'];
    }

    /**
     * @param $tableId
     * @param $options
     * @return int
     */
    public function queueTableExport($tableId, $options = [])
    {
        $job = $this->apiPostJson(
            "tables/{$tableId}/export-async",
            $this->prepareExportOptions($options),
            false
        );
        return $job['id'];
    }

    private function writeTableOptionsPrepare($options)
    {
        $allowedOptions = [
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
        ];

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        return array_merge($filteredOptions, [
            'incremental' => isset($options['incremental']) ? (bool) $options['incremental'] : false,
        ]);
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
        return $this->apiGet('tables/' . $tableId);
    }

    /**
     *
     * Drop a table
     *
     * @param string $tableId
     * @param array $options - (bool) force
     * @return mixed|string
     */
    public function dropTable($tableId, $options = [])
    {
        $url = 'tables/' . $tableId;

        $allowedOptions = [
            'force',
        ];

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
        $data = [
            'value' => $value,
        ];
        if ($protected !== null) {
            $data['protected'] = (bool) $protected;
        }
        // Keep form-data, doesn't support JSON - endpoint will be removed
        $this->apiPost("tables/$tableId/attributes/$key", $data);
    }

    /**
     * @deprecated
     * @param $tableId
     * @param array $attributes array of objects with `name`, `value`, `protected` keys
     */
    public function replaceTableAttributes($tableId, $attributes = [])
    {
        $params = [];
        if (!empty($attributes)) {
            $params['attributes'] = $attributes;
        }
        // Keep form-data, doesn't support JSON - endpoint will be removed
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
    public function addTableColumn(string $tableId, string $name, ?array $definition = null, ?string $basetype = null)
    {
        $data = [
            'name' => $name,
        ];
        if ($definition !== null) {
            $data['definition'] = $definition;
        }
        if ($basetype !== null) {
            $data['basetype'] = $basetype;
        }
        $this->apiPostJson("tables/$tableId/columns", $data);
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
    public function deleteTableColumn($tableId, $name, $options = [])
    {
        $url = "tables/$tableId/columns/$name";

        $allowedOptions = [
            'force',
        ];

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
        return $this->apiGet('search/tables?' . http_build_query($options->toArray()));
    }


    public function searchComponents(SearchComponentConfigurationsOptions $options)
    {
        return $this->apiGet(
            'search/component-configurations?' . http_build_query($options->toParamsArray())
        );
    }

    /**
     * @param $jobId
     * @return array
     */
    public function getJob($jobId)
    {
        return $this->apiGet('jobs/' . $jobId);
    }

    public function listJobs($options = [])
    {
        return $this->apiGet('jobs?' . http_build_query($options), null, [Client::REQUEST_OPTION_EXTENDED_TIMEOUT=>true]);
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
        return $this->tokens->getToken((int) $tokenId);
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
     * @return array
     */
    public function verifyToken()
    {
        return (array) $this->apiGet('tokens/verify');
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

        $this->log("Token {$result["id"]} created", ['options' => $options->toParamsArray(), 'result' => $result]);

        return $result['id'];
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
            'options' => $options->toParamsArray(),
            'result' => $result,
        ]);

        return $result['id'];
    }

    /**
     * @param string $tokenId
     * @return string
     * @deprecated Will be removed in next major release. Use Tokens::dropToken()
     */
    public function dropToken($tokenId)
    {
        $this->tokens->dropToken((int) $tokenId);
        $this->log("Token {$tokenId} deleted");
        return ''; // BC
    }

    /**
     *
     * Refreshes a token. If refreshing current token, the token is updated.
     *
     * @param string|null $tokenId If not set, defaults to self
     * @return string new token
     * @deprecated $tokenId parameter will be removed in next major release. Use Tokens::refreshToken()
     */
    public function refreshToken($tokenId = null)
    {
        $currentToken = $this->verifyToken();
        if ($tokenId == null) {
            $tokenId = $currentToken['id'];
        }

        $result = $this->tokens->refreshToken((int) $tokenId);

        if ($currentToken['id'] == $result['id']) {
            $this->token = $result['token'];
        }

        $this->log("Token {$tokenId} refreshed", ['token' => $result]);

        return $result['token'];
    }

    /**
     * @param string $tokenId
     * @param string $recipientEmail
     * @param string $message
     * @return void
     * @deprecated Will be removed in next major release. Use Tokens::shareToken()
     */
    public function shareToken($tokenId, $recipientEmail, $message)
    {
        $this->tokens->shareToken((int) $tokenId, $recipientEmail, $message);
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
    public function getTableDataPreview($tableId, $options = [])
    {
        $url = "tables/{$tableId}/data-preview";
        $url .= '?' . http_build_query($this->prepareExportOptions($options));

        return $this->apiGet($url, null, [Client::REQUEST_OPTION_EXTENDED_TIMEOUT => true]);
    }

    /**
     * Exports table content into File Uploads asynchronously. Waits for async operation result. Created file id is
     * stored in returned job results.
     * http://docs.keboola.apiary.io/#post-%2Fv2%2Fstorage%2Ftables%2F%7Btable_id%7D%2Fexport-async
     *
     * @param string $tableId
     * @param array<mixed> $options
     *    - (int) limit,
     *  - (timestamp | strtotime format) changedSince
     *  - (timestamp | strtotime format) changedUntil
     *  - (bool) escape
     *  - (array) columns
     * @return array job results
     */
    public function exportTableAsync($tableId, $options = [])
    {
        return $this->apiPostJson(
            "tables/{$tableId}/export-async",
            $this->prepareExportOptions($options),
        );
    }

    private function prepareExportOptions(array $options)
    {
        $allowedOptions = [
            'limit',
            'changedSince',
            'changedUntil',
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
            $filteredOptions['columns'] = implode(',', (array) $options['columns']);
        }

        if (isset($options['whereValues'])) {
            $filteredOptions['whereValues'] = (array) $options['whereValues'];
        }

        return $filteredOptions;
    }

    /**
     * @param array $options
     * @return array
     */
    private function deleteTableRowsPrepareOptions($options = []): array
    {
        $allowedOptions = [
            'changedSince',
            'changedUntil',
            'whereColumn',
            'whereOperator',
        ];

        $filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

        if (isset($options['whereValues'])) {
            $filteredOptions['whereValues'] = (array) $options['whereValues'];
        }

        if (isset($options['whereFilters'])) {
            $filteredOptions['whereFilters'] = (array) $options['whereFilters'];
        }

        return $filteredOptions;
    }

    /**
     * @param string $tableId
     * @param array $options
     * @return mixed|string
     */
    public function deleteTableRows($tableId, $options = [])
    {
        $filteredOptions = $this->deleteTableRowsPrepareOptions($options);

        return $this->apiDeleteParamsJson("tables/{$tableId}/rows", $filteredOptions);
    }

    /**
     * @deprecated use self::deleteTableRows instead
     * @param string $tableId
     * @param array $options
     * @return mixed|string
     */
    public function deleteTableRowsAsQuery($tableId, $options = [])
    {
        $filteredOptions = $this->deleteTableRowsPrepareOptions($options);

        $url = sprintf('tables/%s/rows?%s', $tableId, http_build_query($filteredOptions));
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
            throw new ClientException('File is not readable: ' . $filePath, null, null, 'fileNotReadable');
        }
        $newOptions = clone $options;
        $fs = null;
        $currentUploadDir = null;
        if ($newOptions->getCompress() && !in_array(strtolower(pathinfo($filePath, PATHINFO_EXTENSION)), ['gzip', 'gz', 'zip'])) {
            $fs = new Filesystem();
            $sapiClientTempDir = sys_get_temp_dir();
            if (!$fs->exists($sapiClientTempDir)) {
                $fs->mkdir($sapiClientTempDir);
            }

            $currentUploadDir = $sapiClientTempDir . '/' . uniqid('file-upload');
            $fs->mkdir($currentUploadDir);

            // gzip file and preserve it's base name
            $gzFilePath = $currentUploadDir . '/' . basename($filePath) . '.gz';
            $command = sprintf('gzip -c %s > %s', escapeshellarg($filePath), escapeshellarg($gzFilePath));

            $process = ProcessPolyfill::createProcess($command);
            $process->setTimeout(null);
            if (0 !== $process->run()) {
                $error = sprintf(
                    'The command "%s" failed.' . "\nExit Code: %s(%s)",
                    $process->getCommandLine(),
                    $process->getExitCode(),
                    $process->getExitCodeText()
                );
                throw new ClientException('Failed to gzip file. ' . $error);
            }
            $filePath = $gzFilePath;
        }
        $sizeBytes = filesize($filePath);
        $newOptions
            ->setFileName(basename($filePath))
            ->setSizeBytes($sizeBytes)
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
                break;
            case self::FILE_PROVIDER_GCP:
                $this->uploadFileToGcs(
                    $prepareResult,
                    $filePath,
                    $newOptions->getIsPermanent(),
                );
                break;
            default:
                throw new Exception('Invalid File Provider: ' . $prepareResult['provider']);
        }

        if ($fs) {
            $fs->remove($currentUploadDir);
        }

        return $prepareResult['id'];
    }

    private function uploadFileToGcs(
        array $prepareResult,
        string $filePath,
        bool $isPermanent
    ): void {
        $gcsUploader = new GCSUploader([
            'credentials' => [
                 'access_token' => $prepareResult['gcsUploadParams']['access_token'],
                 'expires_in' => $prepareResult['gcsUploadParams']['expires_in'],
                 'token_type' => $prepareResult['gcsUploadParams']['token_type'],
                ],
            'projectId' => $prepareResult['gcsUploadParams']['projectId'],
        ]);

        $gcsUploader->uploadFile(
            $prepareResult['gcsUploadParams']['bucket'],
            $prepareResult['gcsUploadParams']['key'],
            $filePath,
            $isPermanent
        );
    }

    /**
     * @param array $prepareResult
     * @param string $filePath
     */
    private function uploadFileToAbs(
        array $prepareResult,
        $filePath
    ) {
        $blobClient = BlobClientFactory::createClientFromConnectionString(
            $prepareResult['absUploadParams']['absCredentials']['SASConnectionString']
        );

        $parallel = true;
        $options = new CommitBlobBlocksOptions();
        if (!$prepareResult['sizeBytes']) {
            // cannot upload empty file in parallel, needs to be created directly
            $options = new CreateBlockBlobOptions();
            $parallel = false;
        }
        $options->setContentDisposition(
            sprintf('attachment; filename=%s', $prepareResult['name'])
        );

        $uploader = new ABSUploader($blobClient);
        $uploader->uploadFile(
            $prepareResult['absUploadParams']['container'],
            $prepareResult['absUploadParams']['blobName'],
            $filePath,
            $options,
            $parallel
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
            'retries' => 40,
            'region' => $prepareResult['region'],
            'http' => [
                'connect_timeout' => 10,
                'timeout' => 500,
            ],
            'debug' => false,
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ],
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
                'http' => true,
            ];
        }

        $s3Client = new S3Client($s3options);
        $s3Uploader = new S3Uploader($s3Client, $transferOptions, $this->logger);
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
            throw new ClientException('File is not sliced.');
        }
        if (!$options->getFileName()) {
            throw new ClientException('File name for sliced file upload not set.');
        }

        $newOptions = clone $options;
        $fs = null;
        $currentUploadDir = null;
        $fs = new Filesystem();
        $sapiClientTempDir = sys_get_temp_dir();
        if (!$fs->exists($sapiClientTempDir)) {
            $fs->mkdir($sapiClientTempDir);
        }
        $currentUploadDir = $sapiClientTempDir . '/' . uniqid('file-upload');
        $fs->mkdir($currentUploadDir);

        if ($newOptions->getCompress()) {
            foreach ($slices as $key => $filePath) {
                if (!in_array(strtolower(pathinfo($filePath, PATHINFO_EXTENSION)), ['gzip', 'gz', 'zip'])) {
                    // gzip file and preserve it's base name
                    $gzFilePath = $currentUploadDir . '/' . basename($filePath) . '.gz';
                    $command = sprintf('gzip -c %s > %s', escapeshellarg($filePath), escapeshellarg($gzFilePath));
                    $process = ProcessPolyfill::createProcess($command);
                    $process->setTimeout(null);
                    if (0 !== $process->run()) {
                        $error = sprintf(
                            'The command "%s" failed.' . "\nExit Code: %s(%s)",
                            $process->getCommandLine(),
                            $process->getExitCode(),
                            $process->getExitCodeText()
                        );
                        throw new ClientException('Failed to gzip file. ' . $error);
                    }
                    $slices[$key] = $gzFilePath;
                }
            }
            $newOptions->setFileName($newOptions->getFileName() . '.gz');
        }

        $fileSize = 0;
        foreach ($slices as $filePath) {
            if (!is_readable($filePath)) {
                throw new ClientException('File is not readable: ' . $filePath, null, null, 'fileNotReadable');
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
            case self::FILE_PROVIDER_GCP:
                $this->uploadSlicedFileToGcs($prepareResult, $slices, $options, $transferOptions);
                break;
            default:
                throw new Exception('Invalid File Provider: ' . $prepareResult['provider']);
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
            'retries' => 40,
            'region' => $preparedFileResult['region'],
            'debug' => false,
            'http' => [
                'connect_timeout' => 10,
                'timeout' => 120,
            ],
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ],
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
                'http' => true,
            ];
        }

        $s3Client = new S3Client($options);
        $s3Uploader = new S3Uploader($s3Client, $transferOptions, $this->logger);
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
                'url' => 's3://' . $uploadParams['bucket'] . '/' . $uploadParams['key'] . basename($filePath),
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

    private function uploadSlicedFileToGcs(
        array $preparedFileResult,
        array $slices,
        FileUploadOptions $newOptions,
        FileUploadTransferOptions $transferOptions = null
    ): void {
        $uploadParams = $preparedFileResult['gcsUploadParams'];
        $gcsUploader = new GCSUploader(
            [
                'credentials' => [
                    'access_token' => $uploadParams['access_token'],
                    'expires_in' => $uploadParams['expires_in'],
                    'token_type' => $uploadParams['token_type'],
                ],
                'projectId' => $uploadParams['projectId'],
            ],
            $this->logger,
            $transferOptions
        );

        $gcsUploader->uploadSlicedFile(
            $uploadParams['bucket'],
            $uploadParams['key'],
            $slices
        );
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
            case self::FILE_PROVIDER_GCP:
                $this->downloadGcsFile($fileInfo, $destination);
                break;
            default:
                throw new Exception('Invalid File Provider: ' . $fileInfo['provider']);
        }
    }

    private function downloadAbsFile(array $fileInfo, $destination)
    {
        $blobClient = BlobClientFactory::createClientFromConnectionString(
            $fileInfo['absCredentials']['SASConnectionString']
        );

        try {
            $getResult = $blobClient->getBlob($fileInfo['absPath']['container'], $fileInfo['absPath']['name']);
        } catch (ServiceException $e) {
            if (!in_array('BlobNotFound', $e->getResponse()->getHeader('x-ms-error-code'))) {
                throw $e;
            }

            throw new ClientException(
                sprintf(self::ERROR_CANNOT_DOWNLOAD_FILE, $fileInfo['name'], $fileInfo['id']),
                404,
                $e
            );
        }

        (new Filesystem())->dumpFile($destination, $getResult->getContentStream());
    }

    private function downloadS3File(array $fileInfo, $destination)
    {
        $s3Client = new S3Client([
            'version' => 'latest',
            'region' => $fileInfo['region'],
            'retries' => 40,
            'http' => [
                'read_timeout' => 10,
                'connect_timeout' => 10,
                'timeout' => 500,
            ],
            'credentials' => [
                'key' => $fileInfo['credentials']['AccessKeyId'],
                'secret' => $fileInfo['credentials']['SecretAccessKey'],
                'token' => $fileInfo['credentials']['SessionToken'],
            ],
        ]);

        try {
            $s3Client->getObject([
                'Bucket' => $fileInfo['s3Path']['bucket'],
                'Key' => $fileInfo['s3Path']['key'],
                'SaveAs' => $destination,
            ]);
        } catch (S3Exception $e) {
            if ($e->getAwsErrorCode() !== 'NoSuchKey') {
                throw $e;
            }

            throw new ClientException(
                sprintf(self::ERROR_CANNOT_DOWNLOAD_FILE, $fileInfo['name'], $fileInfo['id']),
                404,
                $e
            );
        }
    }

    private function downloadGcsFile(array $fileInfo, string $destination): void
    {
        $options = [
            'credentials' => [
                'access_token' => $fileInfo['gcsCredentials']['access_token'],
                'expires_in' => $fileInfo['gcsCredentials']['expires_in'],
                'token_type' => $fileInfo['gcsCredentials']['token_type'],
            ],
            'projectId' => $fileInfo['gcsCredentials']['projectId'],
        ];

        $fetchAuthToken = $this->getAuthTokenClass($options['credentials']);
        $gcsClient = new GoogleStorageClient([
            'projectId' => $options['projectId'],
            'credentialsFetcher' => $fetchAuthToken,
        ]);

        $retBucket = $gcsClient->bucket($fileInfo['gcsPath']['bucket']);
        $object = $retBucket->object($fileInfo['gcsPath']['key']);
        try {
            $object->downloadToFile($destination);
        } catch (NotFoundException $e) {
            throw new ClientException(
                sprintf(self::ERROR_CANNOT_DOWNLOAD_FILE, $fileInfo['name'], $fileInfo['id']),
                404,
                $e,
            );
        }
    }

    public function downloadSlicedFile($fileId, $destinationFolder)
    {
        $fileInfo = $this->getFile($fileId, (new GetFileOptions())->setFederationToken(true));
        switch ($fileInfo['provider']) {
            case self::FILE_PROVIDER_AZURE:
                return $this->downloadAbsSlicedFile($fileInfo, $destinationFolder);
            case self::FILE_PROVIDER_AWS:
                return $this->downloadS3SlicedFile($fileInfo, $destinationFolder);
            case self::FILE_PROVIDER_GCP:
                return $this->downloadGcsSlicedFile($fileInfo, $destinationFolder);
            default:
                throw new Exception('Invalid File Provider: ' . $fileInfo['provider']);
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

        try {
            $getResult = $blobClient->getblob($fileInfo['absPath']['container'], $fileInfo['absPath']['name'] . 'manifest');
            /** @var array{entries: array{url: string}} $manifest */
            $manifest = Utils::jsonDecode((string) stream_get_contents($getResult->getContentStream()), true);
            $slices = [];
            $fs = new Filesystem();
            /** @var array{url:string} $entry */
            foreach ($manifest['entries'] as $entry) {
                $blobPath = explode(sprintf(
                    'blob.core.windows.net/%s/',
                    $fileInfo['absPath']['container']
                ), $entry['url'])[1];
                $getResult = $blobClient->getBlob($fileInfo['absPath']['container'], $blobPath);
                $slices[] = $destinationFile = $destinationFolder . basename($entry['url']);
                $fs->dumpFile($destinationFile, $getResult->getContentStream());
            }
        } catch (ServiceException $e) {
            if (!in_array('BlobNotFound', $e->getResponse()->getHeader('x-ms-error-code'))) {
                throw $e;
            }

            throw new ClientException(
                sprintf(self::ERROR_CANNOT_DOWNLOAD_FILE, $fileInfo['name'], $fileInfo['id']),
                404,
                $e
            );
        }

        return $slices;
    }

    private function downloadS3SlicedFile(array $fileInfo, $destinationFolder)
    {
        $s3Client = new S3Client([
            'version' => 'latest',
            'region' => $fileInfo['region'],
            'retries' => 40,
            'http' => [
                'connect_timeout' => 10,
                'timeout' => 120,
            ],
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

        try {
            /** @var array{Body:string} $object */
            $object = $s3Client->getObject([
                'Bucket' => $fileInfo['s3Path']['bucket'],
                'Key' => $fileInfo['s3Path']['key'] . 'manifest',
            ]);
            /** @var array{entries: array{url: string}} $manifest */
            $manifest = Utils::jsonDecode($object['Body'], true);
            $slices = [];
            /** @var array{url: string} $entry */
            foreach ($manifest['entries'] as $entry) {
                $object = $s3Client->getObject([
                    'Bucket' => $fileInfo['s3Path']['bucket'],
                    'Key' => strtr($entry['url'], ['s3://' . $fileInfo['s3Path']['bucket'] . '/' => '']),
                ]);
                $slices[] = $destinationFile = $destinationFolder . basename($entry['url']);
                file_put_contents($destinationFile, $object['Body']);
            }
        } catch (S3Exception $e) {
            if ($e->getAwsErrorCode() !== 'NoSuchKey') {
                throw $e;
            }

            throw new ClientException(
                sprintf(self::ERROR_CANNOT_DOWNLOAD_FILE, $fileInfo['name'], $fileInfo['id']),
                404,
                $e
            );
        }

        return $slices;
    }

    private function downloadGcsSlicedFile(array $fileInfo, string $destinationFolder): array
    {
        $options = [
            'credentials' => [
                'access_token' => $fileInfo['gcsCredentials']['access_token'],
                'expires_in' => $fileInfo['gcsCredentials']['expires_in'],
                'token_type' => $fileInfo['gcsCredentials']['token_type'],
            ],
            'projectId' => $fileInfo['gcsCredentials']['projectId'],
        ];

        $fetchAuthToken = $this->getAuthTokenClass($options['credentials']);
        $gcsClient = new GoogleStorageClient([
            'projectId' => $options['projectId'],
            'credentialsFetcher' => $fetchAuthToken,
        ]);

        $retBucket = $gcsClient->bucket($fileInfo['gcsPath']['bucket']);

        if (!file_exists($destinationFolder)) {
            $fs = new Filesystem();
            $fs->mkdir($destinationFolder);
        }

        if (substr($destinationFolder, -1) !== '/') {
            $destinationFolder .= '/';
        }

        try {
            $manifestObject = $retBucket->object($fileInfo['gcsPath']['key'] . 'manifest')->downloadAsString();
            /** @var array{entries: array{url: string}} $manifest */
            $manifest = Utils::jsonDecode($manifestObject, true);
            $slices = [];
            /** @var array{url: string} $entry */
            foreach ($manifest['entries'] as $entry) {
                $slices[] = $destinationFile = $destinationFolder . basename($entry['url']);

                $sprintf = sprintf(
                    '/%s/',
                    $fileInfo['gcsPath']['bucket']
                );
                $blobPath = explode($sprintf, $entry['url']);
                $retBucket->object($blobPath[1])->downloadToFile($destinationFile);
            }
        } catch (NotFoundException $e) {
            throw new ClientException(
                sprintf(self::ERROR_CANNOT_DOWNLOAD_FILE, $fileInfo['name'], $fileInfo['id']),
                404,
                $e,
            );
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
        return $this->apiPostJson('files/prepare', [
            'isPublic' => $options->getIsPublic(),
            'isPermanent' => $options->getIsPermanent(),
            'isEncrypted' => $options->getIsEncrypted(),
            'isSliced' => $options->getIsSliced(),
            'notify' => $options->getNotify(),
            'name' => $options->getFileName(),
            'sizeBytes' => $options->getSizeBytes(),
            'tags' => $options->getTags(),
            'federationToken' => $options->getFederationToken(),
        ]);
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
     * @param string|int $fileId
     * @return array
     */
    public function getFile($fileId, GetFileOptions $options = null)
    {
        if (empty($fileId)) {
            throw new ClientException('File id cannot be empty');
        }
        return $this->apiGet("files/$fileId?" . http_build_query($options ? $options->toArray() : []));
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
        $this->apiPostJson("files/$fileId/tags", [
            'tag' => $tagName,
        ]);
    }

    /**
     * List files
     *
     * @param ListFilesOptions $options
     * @return array
     */
    public function listFiles(ListFilesOptions $options = null)
    {
        return $this->apiGet('files?' . http_build_query($options ? $options->toArray() : []));
    }

    protected function prepareDataForCreateEvent(Event $event): array
    {
        return [
            'component' => $event->getComponent(),
            'configurationId' => $event->getConfigurationId(),
            'runId' => $event->getRunId(),
            'message' => $event->getMessage(),
            'description' => $event->getDescription(),
            'type' => $event->getType(),
            'params' => json_encode($event->getParams()),
            'results' => json_encode($event->getResults()),
            'duration' => $event->getDuration(),
        ];
    }

    /**
     * Create new event
     *
     * @param Event $event
     * @return int - created event id
     */
    public function createEvent(Event $event)
    {
        $result = $this->apiPostJson('events', $this->prepareDataForCreateEvent($event));
        assert(is_array($result));
        return $result['id'];
    }

    /**
     * Create new event with form-data request
     *
     * @param Event $event
     * @return int - created event id
     */
    public function createEventWithFormData(Event $event)
    {
        $result = $this->apiPost('events', $this->prepareDataForCreateEvent($event));
        assert(is_array($result));
        return $result['id'];
    }

    /**
     * @param int|string $id
     * @return array
     */
    public function getEvent($id)
    {
        /** @var array $response */
        $response = $this->apiGet('events/' . $id);
        return $response;
    }

    /**
     * @param array $params
     * @return array
     */
    public function listEvents($params = [])
    {
        $defaultParams = [
            'limit' => 100,
            'offset' => 0,
        ];

        if (!is_array($params)) {
            // BC compatibility
            $args = func_get_args();
            $params = [
                'limit' => $args[0],
            ];
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
        $defaultParams = [
            'limit' => 100,
            'offset' => 0,
        ];

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
        $defaultParams = [
            'limit' => 100,
            'offset' => 0,
        ];

        $queryParams = array_merge($defaultParams, $params);
        return $this->apiGet("buckets/{$bucketId}/events?" . http_build_query($queryParams));
    }

    /**
     * @param int $tokenId
     * @param array $params
     * @return array
     */
    public function listTokenEvents($tokenId, $params = [])
    {
        $defaultParams = [
            'limit' => 100,
            'offset' => 0,
        ];

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
     * @return string generated id
     * @phpstan-return numeric-string
     */
    public function generateId()
    {
        $result = $this->apiPostJson('tickets');
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
     * @param string|null $fileName
     * @param array $requestOptions
     * @return mixed|string|array
     */
    public function apiGet($url, $fileName = null, $requestOptions = [])
    {
        $requestOptions = $this->filterRequestOptions($requestOptions);
        return $this->request('GET', $url, $requestOptions, $fileName);
    }

    /**
     * Prepare URL and call a POST request
     *
     * @param string $url
     * @param array|null $postData
     * @param bool $handleAsyncTask
     * @param array $requestOptions
     * @return mixed|string
     * @deprecated use apiPostJson method
     */
    public function apiPost($url, $postData = null, $handleAsyncTask = true, $requestOptions = [])
    {
        $requestOptions = $this->filterRequestOptions($requestOptions);
        $requestOptions['form_params'] = $postData;
        return $this->request('POST', $url, $requestOptions, null, $handleAsyncTask);
    }

    /**
     * @param string $url
     * @param array|null $postData
     * @param bool $handleAsyncTask
     * @return mixed|string
     */
    public function apiPostMultipart($url, $postData = null, $handleAsyncTask = true)
    {
        return $this->request('post', $url, ['multipart' => $postData], null, $handleAsyncTask);
    }

    /**
     * @param string $url
     * @param array $data
     */
    public function apiPostJson($url, $data = [], bool $handleAsyncTask = true, array $requestOptions = [])
    {
        $requestOptions = $this->filterRequestOptions($requestOptions);
        $requestOptions['json'] = $data;
        return $this->request('POST', $url, $requestOptions, null, $handleAsyncTask);
    }

    /**
     * @param string $url
     * @param array|null $data
     * @return mixed|string
     * @deprecated use apiPutJson method
     */
    public function apiPut($url, $data = null)
    {
        return $this->request('PUT', $url, [
            'form_params' => $data,
        ]);
    }

    /**
     * @return mixed|string
     */
    public function apiPutJson(string $url, array $data = [])
    {
        return $this->request('PUT', $url, [
            'json' => $data,
        ]);
    }

    /**
     * @param string $url
     * @return mixed|string
     */
    public function apiDelete($url)
    {
        return $this->request('DELETE', $url);
    }

    /**
     * @param string $url
     * @param array $data
     * @return mixed|string
     * @deprecated use apiDeleteParamsJson method
     */
    public function apiDeleteParams($url, $data)
    {
        return $this->request('DELETE', $url, [
            'form_params' => $data,
        ]);
    }

    /**
     * @return mixed|string
     */
    public function apiDeleteParamsJson(string $url, array $data = [])
    {
        return $this->request('DELETE', $url, [
            'json' => $data,
        ]);
    }

    protected function request($method, $url, $options = [], $responseFileName = null, $handleAsyncTask = true)
    {
        $url = self::API_VERSION . '/storage/' . $url;
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

        if ($this->getBackendConfiguration()) {
            $requestOptions['headers']['X-KBC-Backend'] = $this->getBackendConfiguration()->toJson();
        }

        if (isset($requestOptions['json']) && is_array($requestOptions['json']) && empty($requestOptions['json'])) {
            // if empty array -> send object `{}` instead of list `[]`
            $requestOptions['json'] = (object) [];
        }

        try {
            /**
             * @var ResponseInterface $response
             */
            $response = $this->client->request($method, $url, $requestOptions);
        } catch (RequestException $e) {
            $response = $e->getResponse();
            $body = $response ? json_decode((string) $response->getBody(), true) : [];

            if ($response && $response->getStatusCode() == 503) {
                throw new MaintenanceException(isset($body['reason']) ? $body['reason'] : 'Maintenance', $response && $response->hasHeader('Retry-After') ? (string) $response->getHeader('Retry-After')[0] : null, $body);
            }

            throw new ClientException(
                isset($body['error']) ? $body['error'] : $e->getMessage(),
                $response ? $response->getStatusCode() : $e->getCode(),
                $e,
                isset($body['code']) ? $body['code'] : '',
                $body
            );
        }

        // wait for asynchronous task completion
        if ($handleAsyncTask && $response->getStatusCode() == 202) {
            return $this->handleAsyncTask($response);
        }

        if ($responseFileName) {
            $responseFile = fopen($responseFileName, 'w');
            if (!$responseFile) {
                throw new ClientException("Cannot open file {$responseFileName}");
            }
            $body = $response->getBody();
            while (!$body->eof()) {
                fwrite($responseFile, $body->read(1024 * 10));
            }
            fclose($responseFile);
            return '';
        }

        if ($response->hasHeader('Content-Type') && $response->getHeader('Content-Type')[0] == 'application/json') {
            return json_decode((string) $response->getBody(), true);
        }

        return (string) $response->getBody();
    }

    /**
     * @param Response $jobCreatedResponse
     * @return mixed
     * @throws ClientException
     */
    private function handleAsyncTask(Response $jobCreatedResponse)
    {
        /** @var array{id: int, results: mixed} $job */
        $job = json_decode((string) $jobCreatedResponse->getBody(), true);
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
        } while (!in_array($job['status'], ['success', 'error']));

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
    private function log($message, $context = [])
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
    public static function parseCsv($csvString, $header = true, $delimiter = ',', $enclosure = '"')
    {
        $data = [];
        $headers = [];
        $firstLine = true;

        $tmpFile = tmpfile();
        if ($tmpFile === false) {
            throw new ClientException('Cannot create temp file for CSV parsing');
        }
        fwrite($tmpFile, $csvString);
        rewind($tmpFile);

        if (!$enclosure) {
            $enclosure = chr(0);
        }

        while ($parsedLine = fgetcsv($tmpFile, 0, $delimiter, $enclosure, '"')) {
            if (!$header) {
                $data[] = $parsedLine;
            } else {
                if ($firstLine) {
                    $headers = $parsedLine;
                } else {
                    $lineData = [];
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

    public function setBackendConfiguration(?BackendConfiguration $configuration): self
    {
        $this->backendConfiguration = $configuration;
        return $this;
    }

    public function getBackendConfiguration(): ?BackendConfiguration
    {
        return $this->backendConfiguration;
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
        $components = [];
        if (!isset($data['components'])) {
            return $components;
        }
        foreach ($data['components'] as $component) {
            $components[$component['id']] = $component['uri'];
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
                'contents' => in_array(gettype($value), ['object', 'resource', 'NULL']) ? $value : (string) $value,
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
        $data = [
            'columns' => $columns,
        ];
        $this->apiPostJson("tables/$tableId/primary-key", $data);
    }

    /**
     * @param array $option
     * @return array
     */
    public function createTrigger($option)
    {
        $result = $this->apiPostJson('triggers/', $option);
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $triggerId
     * @param array $options
     * @return array
     */
    public function updateTrigger($triggerId, $options)
    {
        $result = $this->apiPutJson('triggers/' . $triggerId .'/', $options);
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $triggerId
     * @return array
     */
    public function getTrigger($triggerId)
    {
        $result = $this->apiGet('triggers/' . $triggerId .'/');
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $triggerId
     * @return void
     */
    public function deleteTrigger($triggerId)
    {
        $this->apiDelete('triggers/' . $triggerId .'/');
    }

    /**
     * @param array $filter
     * @return array
     */
    public function listTriggers($filter = [])
    {
        $result = $this->apiGet('triggers/?' . http_build_query($filter));
        assert(is_array($result));
        return $result;
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

    /**
     * @param array $requestOptions
     * @return array
     */
    private function filterRequestOptions($requestOptions)
    {
        return array_filter($requestOptions, function ($key) {
            return in_array($key, self::ALLOWED_REQUEST_OPTIONS);
        }, ARRAY_FILTER_USE_KEY);
    }

    private function getAuthTokenClass(array $credentials): FetchAuthTokenInterface
    {
        return new class ($credentials) implements FetchAuthTokenInterface {
            private array $creds;

            public function __construct(
                array $creds
            ) {
                $this->creds = $creds;
            }

            public function fetchAuthToken(callable $httpHandler = null)
            {
                return $this->creds;
            }

            public function getCacheKey()
            {
                return '';
            }

            public function getLastReceivedToken()
            {
                return $this->creds;
            }
        };
    }
}
