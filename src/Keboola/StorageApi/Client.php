<?php
namespace Keboola\StorageApi;



use GuzzleHttp\Event\SubscriberInterface;
use GuzzleHttp\Event\AbstractTransferEvent;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Message\Response;
use GuzzleHttp\Subscriber\Log\LogSubscriber;
use GuzzleHttp\Subscriber\Retry\RetrySubscriber;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;
use Keboola\Csv\CsvFile,
	Keboola\StorageApi\Options\FileUploadOptions;

class Client
{
	// Stage names
	const STAGE_IN = "in";
	const STAGE_OUT = "out";
	const STAGE_SYS = "sys";

	const VERSION = '2.11.27';

	// Token string
	public $token;

	// Token object
	private $tokenObj = null;

	// current run id sent with all request
	private $runId = null;

	// API URL
	private $apiUrl = "https://connection.keboola.com";

	private $apiVersion = "v2";

	private $backoffMaxTries = 11;

	// User agent header send with each API request
	private $userAgent = 'Keboola Storage API PHP Client';

	/**
	 * Log callback
	 * @var callable
	 */
	private $log;

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
	 *     - url: (optional) Storage API URL
	 *     - userAgent: custom user agent
	 *     - backoffMaxTries: backoff maximum number of attempts
	 *     - logger: instance of LoggerInterface
	 *     - eventSubscriber: instance of SubscriberInterface
	 */
	public function __construct(array $config = array())
	{
		if (isset($config['url'])) {
			$this->apiUrl = $config['url'];
		}

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

		if (isset($config['logger'])) {
			$this->setLogger($config['logger']);
		} else {
			$this->setLogger(new NullLogger());
		}

		$this->initClient();
		$this->initExponentialBackoff();

		if (isset($config['eventSubscriber'])) {
			if (!$config['eventSubscriber'] instanceof SubscriberInterface) {
				throw new \InvalidArgumentException('eventSubscriber must be instance of GuzzleHttp\Event\SubscriberInterface');
			}
			$this->client->getEmitter()->attach($config['eventSubscriber']);
		}
	}

	private function initClient()
	{
		$this->client = new \GuzzleHttp\Client([
			'base_url' => $this->apiUrl,
		]);
		$logSubsriber = new LogSubscriber($this->logger, "{hostname} {req_header_User-Agent} - [{ts}] \"{method} {resource} {protocol}/{version}\" {code} {res_header_Content-Length}");
		$this->client->getEmitter()->attach($logSubsriber);
	}


	private function initExponentialBackoff()
	{
		$this->client->getEmitter()->attach($this->createExponentialBackoffSubsriber());
	}

	private function createExponentialBackoffSubsriber()
	{
		$filter = RetrySubscriber::createChainFilter([
			RetrySubscriber::createCurlFilter(),
			RetrySubscriber::createStatusFilter(),
		]);
		return new RetrySubscriber([
			'filter' => $filter,
			'delay' => RetrySubscriber::createLoggingDelay(['GuzzleHttp\Subscriber\Retry\RetrySubscriber', 'exponentialDelay'], $this->logger),
			'sleep' => function ($time, AbstractTransferEvent $event) {
				usleep($time * 1000 * 1000);
			},
			'max' => $this->backoffMaxTries,
		]);
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
	 * API index with available components list
	 * @return array
	 */
	public function indexAction()
	{
		return $this->apiGet("storage");
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
		return $this->apiGet("storage/buckets?" . http_build_query($options));
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
		foreach($buckets as $bucket) {
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
		return $this->apiGet("storage/buckets/" . $bucketId);
	}

	/**
	 *
	 * Create a bucket. If a bucket exists, return existing bucket URL.
	 *
	 * @param string $name bucket name
	 * @param string $stage bucket stage
	 * @param string $description bucket description
	 *
	 * @return string bucket Id
	 */
	public function createBucket($name, $stage, $description, $backend = null)
	{
		$options = array(
			"name" => $name,
			"stage" => $stage,
			"description" => $description,
		);

		if ($backend) {
			$options['backend'] = $backend;
		}

		$bucketId = $this->getBucketId($name, $stage);
		if ($bucketId) {
			return $bucketId;
		}

		$result = $this->apiPost("storage/buckets", $options);

		$this->log("Bucket {$result["id"]} created", array("options" => $options, "result" => $result));

		return $result["id"];
	}

	/**
	 *
	 * Delete a bucket. Only empty buckets can be deleted
	 *
	 * @param string $bucketId
	 * @return mixed|string
	 */
	public function dropBucket($bucketId)
	{
		return $this->apiDelete("storage/buckets/" . $bucketId);
	}

	/**
	 *
	 * Set a bucket attribute
	 *
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
			$data['protected'] = (bool) $protected;
		}
		$this->apiPost("storage/buckets/$bucketId/attributes/$key", $data);
	}

	/**
	 *
	 * Delete a bucket attribute
	 *
	 * @param string $bucketId
	 * @param string $key
	 * @return mixed|string
	 */
	public function deleteBucketAttribute($bucketId, $key)
	{
		$result = $this->apiDelete("storage/buckets/$bucketId/attributes/$key");
		$this->log("Bucket $bucketId attribute $key deleted");
		return $result;
	}

	/**
	 *
	 * Checks if a bucket exists
	 *
	 * @param string $bucketId
	 * @return bool
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
		);

		if ($this->isUrl($csvFile->getPathname())) {
			$options["dataUrl"] = $csvFile->getPathname();
		} else {
			$options["data"] = fopen($csvFile->getPathname(), 'r');
		}


		$tableId = $this->getTableId($name, $bucketId);
		if ($tableId) {
			return $tableId;
		}
		$result = $this->apiPost("storage/buckets/" . $bucketId . "/tables", $options);

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
			"transactional" => isset($options['transactional']) ? $options['transactional'] : false,
			'columns' => isset($options['columns']) ? $options['columns'] : null,
		);

		if ($this->isUrl($csvFile->getPathname())) {
			$options['dataUrl'] = $csvFile->getPathname();
		} else {
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
		}

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
		$createdTable = $this->apiPost("storage/buckets/{$bucketId}/tables-async", $options);
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

	private function isUrl($path)
	{
		return preg_match('/^https?:\/\/.*$/', $path);
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
	public function createAliasTable($bucketId, $sourceTableId, $name = NULL, $options = array())
	{
		$filteredOptions = array(
			'sourceTable' => $sourceTableId,
			'name' => $name,
		);

		if (isset($options['aliasFilter'])) {
			$filteredOptions['aliasFilter'] = (array) $options['aliasFilter'];
		}

		if (isset($options['aliasColumns'])) {
			$filteredOptions['aliasColumns'] = (array) $options['aliasColumns'];
		}

		$result = $this->apiPost("storage/buckets/" . $bucketId . "/table-aliases", $filteredOptions);
		$this->log("Table alias {$result["id"]}  created", array("options" => $filteredOptions, "result" => $result));
		return $result["id"];
	}

	/**
	 * @param $bucketId
	 * @param $sql
	 * @param null $name
	 * @param null $sourceTableId
	 * @return string - created table id
	 * @throws ClientException
	 */
	public function createRedshiftAliasTable($bucketId, $sql, $name = NULL, $sourceTableId = NULL)
	{
		$filteredOptions = array(
			'selectSql' => $sql,
		);

		if (!$name && !$sourceTableId) {
			throw new ClientException("Either parameter name or parameter sourceTableId must be used");
		}

		if ($name) {
			$filteredOptions['name'] = $name;
		}

		if (isset($sourceTableId)) {
			$filteredOptions['sourceTable'] = $sourceTableId;
		}

		$result = $this->apiPost("storage/buckets/" . $bucketId . "/table-aliases", $filteredOptions);
		$this->log("Table alias {$result["id"]}  created", array("options" => $filteredOptions, "result" => $result));
		return $result["id"];
	}

	public function updateRedshiftAliasTable($tableId, $sql)
	{
		$result = $this->apiPut("storage/tables/" . $tableId, array('selectSql' => $sql));
		return $result;
	}

	/**
	 * @param $tableId
	 * @return int - snapshot id
	 */
	public function createTableSnapshot($tableId, $snapshotDescription = null)
	{
		$result = $this->apiPost("storage/tables/{$tableId}/snapshots", array(
			'description' => $snapshotDescription,
		));
		$this->log("Snapthos {$result['id']} of table {$tableId} created.");
		return $result["id"];
	}

	public function rollbackTableFromSnapshot($tableId, $snapshotId)
	{
		return $this->apiPost("storage/tables/{$tableId}/rollback", array(
			'snapshotId' => $snapshotId,
		));
	}

	/**
	 * @param $tableId
	 * @return mixed|string
	 */
	public function listTableSnapshots($tableId, $options = array())
	{
		return $this->apiGet("storage/tables/{$tableId}/snapshots?" . http_build_query($options));
	}

	/**
	 * @param $tableId
	 * @param array $filter
	 * @return mixed|string
	 */
	public function setAliasTableFilter($tableId, array $filter)
	{
		$result = $this->apiPost("storage/tables/$tableId/alias-filter", $filter);
		$this->log("Table $tableId  filter set", array(
			'filter' => $filter,
			'result' => $result,
		));
		return $result;
	}

	public function removeAliasTableFilter($tableId)
	{
		$this->apiDelete("storage/tables/$tableId/alias-filter");
	}

	/**
	 * @param $tableId
	 */
	public function enableAliasTableColumnsAutoSync($tableId)
	{
		$this->apiPost("storage/tables/{$tableId}/alias-columns-auto-sync");
	}

	/**
	 * @param $tableId
	 */
	public function disableAliasTableColumnsAutoSync($tableId)
	{
		$this->apiDelete("storage/tables/{$tableId}/alias-columns-auto-sync");
	}

	/**
	 *
	 * Get all available tables
	 *
	 * @param string $bucketId limit search to a specific bucket
	 * @param array $options
	 * @return array
	 */
	public function listTables($bucketId=null, $options = array())
	{
		if ($bucketId) {
			return $this->apiGet("storage/buckets/{$bucketId}/tables?" . http_build_query($options));
		}
		return $this->apiGet("storage/tables?" . http_build_query($options));
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
		foreach($tables as $table) {
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
	 * 	Available options:
	 *  - incremental
	 *  - partial
	 * @return mixed|string
	 */
	public function writeTable($tableId, CsvFile $csvFile,  $options = array())
	{
		$optionsExtended = $this->writeTableOptionsPrepare(array_merge($options, array(
			"delimiter" => $csvFile->getDelimiter(),
			"enclosure" => $csvFile->getEnclosure(),
			"escapedBy" => $csvFile->getEscapedBy(),
		)));

		if ($this->isUrl($csvFile->getPathname())) {
			$optionsExtended["dataUrl"] = $csvFile->getPathname();
		} else {
			$optionsExtended["data"] = @fopen($csvFile->getRealPath(), 'r');
			if ($optionsExtended["data"] === false) {
				throw new ClientException("Failed to open temporary data file " . $csvFile->getRealPath(), null, null, 'fileNotReadable');
			}
		}

		$result = $this->apiPost("storage/tables/{$tableId}/import" , $optionsExtended);

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

		if ($this->isUrl($csvFile->getPathname())) {
			$optionsExtended['dataUrl'] = $csvFile->getPathname();
		} else {
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
		}

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
		return $this->apiPost("storage/tables/{$tableId}/import-async", $this->writeTableOptionsPrepare($options));
	}

	private function writeTableOptionsPrepare($options)
	{
		$allowedOptions = array(
			'delimiter',
			'enclosure',
			'escapedBy',
			'dataFileId',
			'dataUrl',
			'dataTableName',
			'data',
			'withoutHeaders',
			'columns',
		);

		$filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

		return array_merge($filteredOptions, array(
			"incremental" => isset($options['incremental']) ? (bool) $options['incremental'] : false,
			"partial" => isset($options['partial']) ? (bool) $options['partial'] : false,
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
		return $this->apiGet("storage/tables/" . $tableId);
	}

	/**
	 *
	 * Drop a table
	 *
	 * @param string $tableId
	 * @return mixed|string
	 */
	public function dropTable($tableId)
	{
		$result = $this->apiDelete("storage/tables/" . $tableId);
		$this->log("Table {$tableId} deleted");
		return $result;
	}

	/**
	 * Unlink aliased table from source table
	 * @param string $tableId
	 * @return mixed|string
	 */
	public function unlinkTable($tableId)
	{
		$result = $this->apiDelete("storage/tables/" . $tableId . '?unlink');
		$this->log("Table {$tableId} unlinked");
		return $result;
	}

	/**
	 *
	 * Set a table attribute
	 *
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
			$data['protected'] = (bool) $protected;
		}
		$this->apiPost("storage/tables/$tableId/attributes/$key", $data);
	}

	/**
	 *
	 * Delete a table attribute
	 *
	 * @param string $tableId
	 * @param string $key
	 * @return mixed|string
	 */
	public function deleteTableAttribute($tableId, $key)
	{
		$result = $this->apiDelete("storage/tables/$tableId/attributes/$key");
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
		$this->apiPost("storage/tables/$tableId/columns", $data);
	}


	/**
	 *
	 * Delete a table attribute
	 *
	 * @param string $tableId
	 * @param string $name
	 * @return mixed|string
	 */
	public function deleteTableColumn($tableId, $name)
	{
		$this->apiDelete("storage/tables/$tableId/columns/$name");
		$this->log("Table $tableId column $name deleted");
	}

	/**
	 *
	 * Add column to table
	 *
	 * @param string $tableId
	 * @param string $columnName
	 */
	public function markTableColumnAsIndexed($tableId, $columnName)
	{
		$data = array(
			'name' => $columnName,
		);
		$this->apiPost("storage/tables/$tableId/indexed-columns", $data);
	}


	/**
	 *
	 * Delete a table attribute
	 *
	 * @param string $tableId
	 * @param string $columnName
	 * @return mixed|string
	 */
	public function removeTableColumnFromIndexed($tableId, $columnName)
	{
		$this->apiDelete("storage/tables/$tableId/indexed-columns/$columnName");
		$this->log("Table $tableId indexed column $columnName deleted");
	}

	/**
	 *
	 * Checks if a table exists
	 *
	 * @param string $tableId
	 * @return bool
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
	 * @param $jobId
	 * @return array
	 */
	public function getJob($jobId)
	{
		return $this->apiGet("storage/jobs/" . $jobId);
	}

	/**
	 *
	 * returns all tokens
	 *
	 * @return array
	 */
	public function listTokens()
	{
		return $this->apiGet("storage/tokens");
	}

	/**
	 *
	 * get token detail
	 *
	 * @param string $tokenId token id
	 * @return array
	 */
	public function getToken($tokenId)
	{
		return $this->apiGet("storage/tokens/" . $tokenId);
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
		return $this->apiGet("storage/tokens/verify");
	}

	/**
	 *
	 * create a new token
	 *
	 * @TODO refactor parameters
	 *
	 * @param array $permissions hash bucketId => permission (read/write) or "manage" for all buckets permissions
	 * @param string null $description
	 * @param integer $expiresIn number of seconds until token expires
	 * @param bool $canReadAllFileUploads
	 * @return integer token id
	 */
	public function createToken($permissions, $description=null, $expiresIn = null, $canReadAllFileUploads = false)
	{
		$options = array();

		if ($permissions == 'manage') {
			$options['canManageBuckets'] = 1;
		} else {
			foreach((array) $permissions as $tableId => $permission) {
				$key = "bucketPermissions[{$tableId}]";
				$options[$key] = $permission;
			}
		}
		if ($description) {
			$options["description"] = $description;
		}
		if ($expiresIn) {
			$options["expiresIn"] = (int) $expiresIn;
		}
		$options['canReadAllFileUploads'] = (bool) $canReadAllFileUploads;

		$result = $this->apiPost("storage/tokens", $options);

		$this->log("Token {$result["id"]} created", array("options" => $options, "result" => $result));

		return $result["id"];
	}

	/**
	 *
	 * update token details
	 *
	 * @param string $tokenId
	 * @param array $permissions
	 * @param string null $description
	 * @return int token id
	 */
	public function updateToken($tokenId, $permissions, $description = null, $canReadAllFileUploads = null)
	{
		$options = array();
		foreach($permissions as $tableId => $permission) {
			$key = "bucketPermissions[{$tableId}]";
			$options[$key] = $permission;
		}
		if ($description) {
			$options["description"] = $description;
		}

		if (!is_null($canReadAllFileUploads)) {
			$options["canReadAllFileUploads"] = (bool) $canReadAllFileUploads;
		}

		$result = $this->apiPut("storage/tokens/" . $tokenId, http_build_query($options));

		$this->log("Token {$tokenId} updated", array("options" => $options, "result" => $result));

		return $tokenId;
	}

	/**
	 * @param string $tokenId
	 * @return mixed|string
	 */
	public function dropToken($tokenId)
	{
		$result = $this->apiDelete("storage/tokens/" . $tokenId);
		$this->log("Token {$tokenId} deleted");
		return $result;
	}

	/**
	 *
	 * Refreshes a token. If refreshing current token, the token is updated.
	 *
	 * @param string $tokenId If not set, defaults to self
	 * @return string new token
	 */
	public function refreshToken($tokenId=null)
	{
		$currentToken = $this->verifyToken();
		if ($tokenId == null) {
			$tokenId = $currentToken["id"];
		}

		$result = $this->apiPost("storage/tokens/" . $tokenId . "/refresh");

		if ($currentToken["id"] == $result["id"]) {
			$this->token = $result['token'];
			$this->tokenObj = $result;
		}

		$this->log("Token {$tokenId} refreshed", array("token" => $result));

		return $result["token"];
	}

	/**
	 * @param $tokenId
	 * @param $recipientEmail
	 * @param $message
	 */
	public function shareToken($tokenId, $recipientEmail, $message)
	{
		$this->apiPost("storage/tokens/$tokenId/share", array(
			'recipientEmail' => $recipientEmail,
			'message' => $message,
		));
	}


	/**
	 * Exports table http://docs.keboola.apiary.io/#get-%2Fv2%2Fstorage%2Ftables%2F%7Btable_id%7D%2Fexport
	 *
	 * @param string $tableId
	 * @param string null $fileName export to file if specified, instead table content is returned
	 * @param array $options all options are optional
	 * 	- (int) limit,
	 *  - (timestamp | strtotime format) changedSince
	 *  - (timestamp | strtotime format) changedUntil
	 *  - (bool) escape
	 *  - (array) columns
	 *  - (string) format - one of rfc, raw, escaped. rfc is default
	 *
	 * @return mixed|string
	 */
	public function exportTable($tableId, $fileName = null, $options = array())
	{
		$url = "storage/tables/{$tableId}/export";
		$url .= '?' . http_build_query($this->prepareExportOptions($options));

		return $this->apiGet($url, $fileName);
	}

	/**
	 * Exports table content into File Uploads asynchronously. Waits for async operation result. Created file id is stored in returned job results.
	 * http://docs.keboola.apiary.io/#post-%2Fv2%2Fstorage%2Ftables%2F%7Btable_id%7D%2Fexport-async
	 *
	 * @param $tableId
	 * @param array $options
	 * 	- (int) limit,
	 *  - (timestamp | strtotime format) changedSince
	 *  - (timestamp | strtotime format) changedUntil
	 *  - (bool) escape
	 *  - (array) columns
	 *  - (string) format - one of rfc, raw, escaped. rfc is default
	 * @return array job results
	 */
	public function exportTableAsync($tableId, $options = array())
	{
		return $this->apiPost(
			"storage/tables/{$tableId}/export-async",
			$this->prepareExportOptions($options)
		);
	}

	private function prepareExportOptions(array $options)
	{
		$allowedOptions = array(
			'limit',
			'changedSince',
			'changedUntil',
			'escape',
			'format',
			'whereColumn',
			'whereOperator',
			'gzip',
		);

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
	 * @param $tableId
	 * @param array $options - (int) limit, (timestamp | strtotime format) changedSince, (timestamp | strtotime format) changedUntil, (bool) escape, (array) columns
	 * @return mixed|string
	 */
	public function deleteTableRows($tableId, $options = array())
	{
		$url = "storage/tables/{$tableId}/rows";

		$allowedOptions = array(
			'changedSince',
			'changedUntil',
			'whereColumn',
			'whereOperator'
		);

		$filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

		if (isset($options['whereValues'])) {
			$filteredOptions['whereValues'] = (array) $options['whereValues'];
		}

		$url .= '?' . http_build_query($filteredOptions);

		return $this->apiDelete($url);
	}

	/**
	 * Upload a file to file uploads
	 *
	 * @param string $filePath
	 * @param FileUploadOptions $options
	 * @return int - created file id
	 */
	public function uploadFile($filePath, FileUploadOptions $options)
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
			exec(sprintf("gzip -c %s > %s", escapeshellarg($filePath), escapeshellarg($gzFilePath)), $output, $ret);
			if ($ret !== 0) {
				throw new ClientException("Failed to gzip file, command return code: " . $ret);
			}
			$filePath = $gzFilePath;
		}
		$newOptions
			->setFileName(basename($filePath))
			->setSizeBytes(filesize($filePath));

		// 1. prepare resource
		$result = $this->prepareFileUpload($newOptions);

		// 2. upload directly do S3 using returned credentials
		$uploadParams = $result['uploadParams'];
		$client = new \GuzzleHttp\Client();
		$client->getEmitter()->attach($this->createExponentialBackoffSubsriber());

		$fh = @fopen($filePath, 'r');
		if ($fh === false) {
			throw new ClientException("Error on file upload to S3: " . $filePath, null, null, 'fileNotReadable');
		}
		try {

			$body = array(
				'key' => $uploadParams['key'],
				'acl' => $uploadParams['acl'],
				'signature' => $uploadParams['signature'],
				'policy' => $uploadParams['policy'],
				'AWSAccessKeyId' => $uploadParams['AWSAccessKeyId'],
				'file' => $fh,
			);
			if ($options->getIsEncrypted()) {
				$body['x-amz-server-side-encryption'] = $uploadParams['x-amz-server-side-encryption'];
			}

			$client->post($uploadParams['url'], array(
				'body' => $body,
			));

		} catch (RequestException $e) {
			$response = $e->getResponse();
			$message = "Error on file upload to S3: " . $e->getMessage();
			if ($response) {
				$message .= ' ' . (string) $response->getBody();
			}
			throw new ClientException($message, $e->getCode(), $e);
		}

		if (is_resource($fh)) {
			fclose($fh);
		}

		if ($fs) {
			$fs->remove($currentUploadDir);
		}

		return $result['id'];
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
		return $this->apiPost("storage/files/prepare", array(
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
		return $this->apiDelete("storage/files/$fileId");
	}


	/**
	 * Get a single file
	 * @param string $fileId
	 * @return array
	 */
	public function getFile($fileId, GetFileOptions $options = null)
	{
		return $this->apiGet("storage/files/$fileId?". http_build_query($options ? $options->toArray() : array()));
	}

	/**
	 * Delete file tag
	 * @param $fileId
	 * @param $tagName
	 */
	public function deleteFileTag($fileId, $tagName)
	{
		$this->apiDelete("storage/files/$fileId/tags/$tagName");
	}

	public function addFileTag($fileId, $tagName)
	{
		$this->apiPost("storage/files/$fileId/tags", array(
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
		return $this->apiGet('storage/files?' . http_build_query($options ? $options->toArray() : array()));
	}


	/**
	 * Create new event
	 *
	 * @param Event $event
	 * @return int - created event id
	 */
	public function createEvent(Event $event)
	{
		$result = $this->apiPost('storage/events', array(
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
		return $this->apiGet('storage/events/' . $id);
	}

	/**
	 * @param int $limit
	 * @param int $offset
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
		return $this->apiGet('storage/events?' . http_build_query($queryParams));
	}

	/**
	 * @param $tableId
	 * @param array $params
	 * @return array
	 */
	public function listTableEvents($tableId, $params = array())
	{
		$defaultParams = array(
			'limit' => 100,
			'offset' => 0,
		);

		$queryParams = array_merge($defaultParams, $params);
		return $this->apiGet("storage/tables/{$tableId}/events?" . http_build_query($queryParams));
	}

	/**
	 * @param $id
	 * @return array
	 */
	public function getSnapshot($id)
	{
		return $this->apiGet("storage/snapshots/$id");
	}

	/**
	 * Unique 64bit sequence generator
	 * @return int generated id
	 */
	public function generateId()
	{
		$result = $this->apiPost('storage/tickets');
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
		return $this->request('GET', $this->versionUrl($url), array(), $fileName);
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
		return $this->request('post', $this->versionUrl($url), array('body' => $postData), null, $handleAsyncTask);
	}

	/**
	 *
	 * Prepare URL and call a POST request
	 *
	 * @param string $url
	 * @param array $postData
	 * @return mixed|stringgit d
	 */
	public function apiPut($url, $postData=null)
	{
		return $this->request('put', $this->versionUrl($url), array(
			'body' => $postData,
			'headers' => array(
				'content-type' => 'application/x-www-form-urlencoded',
			),
		));
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
		return $this->request('delete', $this->versionUrl($url));
	}

	private function versionUrl($path)
	{
		return "{$this->apiVersion}/$path";
	}


	protected function request($method, $url, $options = array(), $responseFileName = null, $handleAsyncTask = true)
	{
		// fix body
		if (isset($options['body']) && is_array($options['body'])) {
			$options['body'] = $this->fixRequestBody($options['body']);
		}

		$request = $this->client->createRequest($method, $url, array_merge(
			$options,
			array(
				'timeout' => $this->getTimeout(),
			)
		));
		$request->addHeaders(array(
			'X-StorageApi-Token' => $this->token,
			'Accept-Encoding' => 'gzip',
			'User-Agent' => $this->getUserAgent(),
		));

		if ($this->getRunId()) {
			$request->addHeader('X-KBC-RunId', $this->getRunId());
		}

		try {
			$response = $this->client->send($request);
		} catch (RequestException $e) {
			$response = $e->getResponse();
			try {
				$body = $response ? $response->json() : array();
			} catch (\GuzzleHttp\Exception\ParseException $e2) {
				$body = array();
			}

			if ($response && $response->getStatusCode() == 503) {
				throw new MaintenanceException(isset($body["reason"]) ? $body['reason'] : 'Maintenance', $response ? (string) $response->getHeader('Retry-After') : null, $body);
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
			$body->seek(0);
			while (!$body->eof()) {
				fwrite($responseFile, $body->read(1024 * 10));
			}
			fclose($responseFile);
			return "";
		}

		if ($response->getHeader('Content-Type') == 'application/json') {
			return $response->json();
		}

		return (string) $response->getBody();
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
		$job = $jobCreatedResponse->json();
		$maxWaitPeriod = 20;
		$retries = 0;

		// poll for status
		do {

			if ($retries > 0) {
				$waitSeconds = min(pow(2, $retries), $maxWaitPeriod);
				sleep($waitSeconds);
			}
			$retries++;

			$job = $this->getJob($job['id']);
		} while(!in_array($job['status'], array('success', 'error')));

		if ($job['status'] == 'error') {
			throw new ClientException(
				$job['error']['message'],
				null,
				null,
				$job['error']['code'],
				$job['error']
			);
		}

		return $job['results'];
	}


	/**
	 * @param string $message Message to log
	 * @param array $context Data to log
	 *
	 */
	private function log($message, $context = array())
	{
		$this->logger->info($message, $context);
	}


	/**
	 *
	 * Prepare data for logs - to avoid having token string directly in logs
	 *
	 * @return array
	 */
	public function getLogData()
	{
		if (!$this->tokenObj) {
			$this->tokenObj = $this->verifyToken();
		}

		$logData = array();
		$logData["token"] = substr($this->tokenObj["token"], 0, 6);
		$logData["owner"] = $this->tokenObj["owner"];
		$logData["id"] = $this->tokenObj["id"];
		$logData["description"] = $this->tokenObj["description"];
		$logData["url"] = $this->apiUrl;
		return $logData;
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
	public static function parseCsv($csvString, $header=true, $delimiter=",", $enclosure='"')
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
					foreach($headers as $i => $headerName) {
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
		foreach($data["components"] as $component) {
			$components[$component["id"]] = $component["uri"];
		}
		return $components;
	}

}
