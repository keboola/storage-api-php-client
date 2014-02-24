<?php
namespace Keboola\StorageApi;


use Guzzle\Http\Curl\CurlHandle;
use Guzzle\Http\Exception\BadResponseException;
use Guzzle\Http\Exception\CurlException;
use Guzzle\Http\Exception\RequestException;
use Guzzle\Http\Message\Request;
use Guzzle\Http\Message\RequestInterface;
use Guzzle\Http\Message\Response;
use Guzzle\Log\ClosureLogAdapter;
use Guzzle\Log\MessageFormatter;
use Guzzle\Plugin\Backoff\BackoffLogger;
use Guzzle\Plugin\Backoff\BackoffPlugin;
use Guzzle\Plugin\Log\LogPlugin;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Symfony\Component\Filesystem\Filesystem;
use Keboola\Csv\CsvFile,
	Keboola\StorageApi\Options\FileUploadOptions,
	Guzzle\Http\Client as GuzzleClient;

class Client
{
	// Stage names
	const STAGE_IN = "in";
	const STAGE_OUT = "out";
	const STAGE_SYS = "sys";

	// Token string
	public $token;

	// Token object
	private $tokenObj = null;

	// curren run id sent with all request
	private $runId = null;

	// API URL
	private $apiUrl = "https://connection.keboola.com";

	private $apiVersion = "v2";

	private $backoffMaxTries;

	// User agent header send with each API request
	private $userAgent = 'Keboola Storage API PHP Client';

	// Log anonymous function
	private static $log;

	/**
	 * @var \Guzzle\Http\Client
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
	 * @param $tokenString
	 * @param string null $url
	 * @param string null $userAgent
	 */
	public function __construct($tokenString, $url=null, $userAgent=null, $backoffMaxTries = 11)
	{
		if ($url) {
			$this->setApiUrl($url);
		}

		if ($userAgent) {
			$this->setUserAgent($userAgent);
		}

		$this->token = $tokenString;
		$this->backoffMaxTries = (int) $backoffMaxTries;
		$this->initClient();
		$this->initExponentialBackoff();
		$this->initLogger();
	}

	private function initClient()
	{
		$this->client = new GuzzleClient($this->getApiBaseUrl(), array(
			'version' => $this->apiVersion
		));
	}

	private function getApiBaseUrl()
	{
		return $this->getApiUrl() . "/{version}";
	}

	private function initExponentialBackoff()
	{
		$backoffPlugin = BackoffPlugin::getExponentialBackoff(
			$this->backoffMaxTries,
			array(500,  503),
			array(
				CURLE_COULDNT_RESOLVE_HOST, CURLE_COULDNT_CONNECT, CURLE_WRITE_ERROR, CURLE_READ_ERROR,
				CURLE_OPERATION_TIMEOUTED, CURLE_SSL_CONNECT_ERROR, CURLE_HTTP_PORT_FAILED, CURLE_GOT_NOTHING,
				CURLE_SEND_ERROR, CURLE_RECV_ERROR, CURLE_PARTIAL_FILE
			)
		);
		$backoffPlugin->setEventDispatcher($this->client->getEventDispatcher());
		$this->client->addSubscriber($backoffPlugin);
	}


	private function initLogger()
	{
		$sapiClient = $this;
		$logAdapter = new ClosureLogAdapter(function($message, $priority, $extras) use ($sapiClient) {
			$params = array();
			if (isset($extras['response']) && $extras['response'] instanceof Response) {
				$params['duration'] = $extras['response']->getInfo('total_time');
			}
			$sapiClient->guzzleLog($message, $params);
		});

		$this->client->addSubscriber(new LogPlugin(
			$logAdapter,
			"HTTP request: [{ts}] \"{method} {resource} {protocol}/{version}\" {code} {res_header_Content-Length}"
		));

		$backoffLogger = new BackoffLogger(
			$logAdapter,
			new MessageFormatter('[{ts}] {method} {url} - {code} {phrase} - Retries: {retries}, Delay: {delay}, cURL: {curl_code} {curl_error}')
		);
		$this->client->addSubscriber($backoffLogger);
	}

	/**
	 * Call private method from closure hack
	 * @param $method
	 * @param $args
	 * @return mixed
	 * @throws \BadMethodCallException
	 */
	public function __call($method, $args) {

		if ($method == 'guzzleLog') {
			return call_user_func_array(array($this, 'log'), $args);
		}
		throw new \BadMethodCallException('Unknown method: ' . $method);
	}

	/**
	 *
	 * Change API Url
	 *
	 * @param string $url
	 */
	public function setApiUrl($url)
	{
		$this->apiUrl = $url;
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
	 * @param string $userAgent
	 * @return Client
	 */
	public function setUserAgent($userAgent)
	{
		$this->userAgent = (string) $userAgent;
		return $this;
	}

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
	 * @return mixed|string
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
	 * @return mixed|string
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
	public function createBucket($name, $stage, $description)
	{
		$options = array(
			"name" => $name,
			"stage" => $stage,
			"description" => $description,
		);

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
	 *  - primaryKey
	 *  - transactional
	 *  - transaction
	 * @return bool|string
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
			"transactional" => isset($options['transactional']) ? $options['transactional'] : false,
		);

		if ($this->isUrl($csvFile->getPathname())) {
			$options["dataUrl"] = $csvFile->getPathname();
		} else {
			$options["data"] = "@{$csvFile->getPathname()}";
		}

		$tableId = $this->getTableId($name, $bucketId);
		if ($tableId) {
			return $tableId;
		}
		$result = $this->apiPost("storage/buckets/" . $bucketId . "/tables", $options);

		$this->log("Table {$result["id"]} created", array("options" => $options, "result" => $result));

		return $result["id"];
	}

	/**
	 * Creates table with header of CSV file, then import whole csv file by async import
	 * @param $bucketId
	 * @param $name
	 * @param CsvFile $csvFile
	 * @param array $options
	 * @return bool|string
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

		$createdTable = $this->apiPost("storage/buckets/{$bucketId}/tables-async", $options);

		return $createdTable['id'];
	}

	/**
	 * @param $bucketId destination bucket
	 * @param $snapshotId source snapshot
	 * @param null $name table name (optional) otherwise fetched from snapshot
	 */
	public function createTableFromSnapshot($bucketId, $snapshotId, $name = null)
	{
		$createdTable = $this->apiPost("storage/buckets/{$bucketId}/tables-async", array(
			'snapshotId' => $snapshotId,
			'name' => $name,
		));
		return $createdTable['id'];
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
	 * @return mixed
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
	 * @param $tableId
	 * @return mixed
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
	 * @return mixed|string
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
	 *  - transaction
	 *  - incremental
	 *  - partial
	 * @return mixed|string
	 */
	public function writeTable($tableId, CsvFile $csvFile,  $options = array())
	{
		// TODO Gzip data
		$options = $this->writeTableOptionsPrepare($csvFile, $options);

		if ($this->isUrl($csvFile->getPathname())) {
			$options["dataUrl"] = $csvFile->getPathname();
		} else {
			$options["data"] = "@{$csvFile->getRealPath()}";
		}

		$result = $this->apiPost("storage/tables/{$tableId}/import" , $options);

		$this->log("Data written to table {$tableId}", array("options" => $options, "result" => $result));

		return $result;
	}

	/**
	 * Write data into table asynchronously and wait for result
	 * @param $tableId
	 * @param CsvFile $csvFile
	 * @param array $options
	 * @return mixed|string
	 */
	public function writeTableAsync($tableId, CsvFile $csvFile, $options = array())
	{
		$options = $this->writeTableOptionsPrepare($csvFile, $options);

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
					->setTags(array('table-import'))
			);
			$options['dataFileId'] = $fileId;
		}

		return $this->apiPost("storage/tables/{$tableId}/import-async", $options);
	}


	private function writeTableOptionsPrepare(CsvFile $csvFile, $options)
	{
		return array(
			"delimiter" => $csvFile->getDelimiter(),
			"enclosure" => $csvFile->getEnclosure(),
			"escapedBy" => $csvFile->getEscapedBy(),
			"transaction" => isset($options['transaction']) ? $options['transaction'] : null,
			"incremental" => isset($options['incremental']) ? (bool) $options['incremental'] : false,
			"partial" => isset($options['partial']) ? (bool) $options['partial'] : false,
		);
	}

	/**
	 *
	 * Get table details
	 *
	 * @param string $tableId
	 * @return mixed
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
	 * @return mixed|string
	 */
	public function getJob($jobId)
	{
		return $this->apiGet("storage/jobs/" . $jobId);
	}

	/**
	 *
	 * returns all tokens
	 *
	 * @return mixed|string
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
	 * @return mixed|string
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
	 * @return mixed
	 */
	public function updateToken($tokenId, $permissions, $description=null)
	{
		$options = array();
		foreach($permissions as $tableId => $permission) {
			$key = "bucketPermissions[{$tableId}]";
			$options[$key] = $permission;
		}
		if ($description) {
			$options["description"] = $description;
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
	 *
	 * Generate GoodData XML configuration for table
	 * TODO Test!
	 *
	 * @param string $tableId
	 * @param string $fileName file to store data
	 * @return mixed|string
	 */
	public function getGdXmlConfig($tableId, $fileName=null)
	{
		return $this->apiGet("storage/tables/{$tableId}/gooddata-xml", null, $fileName);
	}

	/**
	 * @param string $tableId
	 * @param string null $fileName
	 * @param array $options - (int) limit, (timestamp | strtotime format) changedSince, (timestamp | strtotime format) changedUntil, (bool) escape, (array) columns
	 * @return mixed|string
	 */
	public function exportTable($tableId, $fileName = null, $options = array())
	{
		$url = "storage/tables/{$tableId}/export";
		$url .= '?' . http_build_query($this->prepareExportOptions($options));

		return $this->apiGet($url, $fileName);
	}

	/**
	 * @param $tableId
	 * @param array $options
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
	 *
	 * Uploads a file
	 *
	 *
	 *
	 * @param string $filePath
	 * @param bool $isPublic
	 * @return mixed|string
	 */
	public function uploadFile($filePath, FileUploadOptions $options)
	{
		$newOptions = clone $options;
		$compressed = false;
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
			$compressed = true;
		}
		$newOptions
			->setFileName(basename($filePath))
			->setSizeBytes(filesize($filePath));

		// 1. prepare resource
		$result = $this->prepareFileUpload($newOptions);

		// 2. upload directly do S3 using returned credentials
		$uploadParams = $result['uploadParams'];
		$client = new GuzzleClient($uploadParams['url']);
		$client->addSubscriber(BackoffPlugin::getExponentialBackoff());

		try {
			$client->post('/', null, array(
				'key' => $uploadParams['key'],
				'acl' => $uploadParams['acl'],
				'signature' => $uploadParams['signature'],
				'policy' => $uploadParams['policy'],
				'AWSAccessKeyId' => $uploadParams['AWSAccessKeyId'],
				'file' => "@$filePath",
			))->send();
		} catch (RequestException $e) {
			throw new ClientException("Error on file upload to S3: " . $e->getMessage(), $e->getCode(), $e);
		}

		if ($compressed) {
			$fs->remove($currentUploadDir);
		}

		return $result['id'];
	}

	public function prepareFileUpload(FileUploadOptions $options)
	{
		return $this->apiPost("storage/files/prepare", array(
			'isPublic' => $options->getIsPublic(),
			'isPermanent' => $options->getIsPermanent(),
			'notify' => $options->getNotify(),
			'name' => $options->getFileName(),
			'sizeBytes' => $options->getSizeBytes(),
			'tags' => $options->getTags(),
			'federationToken' => $options->getFederationToken(),
		));
	}

	/**
	 * Get a single file
	 * @param string $fileId
	 * @return mixed|string
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
	 * Files list
	 */
	public function listFiles(ListFilesOptions $options = null)
	{
		return $this->apiGet('storage/files?' . http_build_query($options ? $options->toArray() : array()));
	}


	/**
	 * Create new event
	 * @param Event $event
	 * @return mixed|string
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
	 * @return mixed|string
	 */
	public function getEvent($id)
	{
		return $this->apiGet('storage/events/' . $id);
	}

	/**
	 * @param int $limit
	 * @param int $offset
	 * @return mixed|string
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

	public function listTableEvents($tableId, $params = array())
	{
		$defaultParams = array(
			'limit' => 100,
			'offset' => 0,
		);

		$queryParams = array_merge($defaultParams, $params);
		return $this->apiGet("storage/tables/{$tableId}/events?" . http_build_query($queryParams));
	}

	public function getSnapshot($id)
	{
		return $this->apiGet("storage/snapshots/$id");
	}

	/**
	 * Unique 64bit sequence generator
	 * @return mixed
	 */
	public function generateId()
	{
		$result = $this->apiPost('storage/tickets');
		return $result['id'];
	}

	/**
	 *
	 * Prepare URL and call a GET request
	 *
	 * @param string $url
	 * @param string null $fileName
	 * @return mixed|string
	 */
	protected function apiGet($url, $fileName=null)
	{
		return $this->request($this->client->get($url), $fileName);
	}

	protected function request(RequestInterface $request, $responseFileName = null )
	{
		$this->client
			->setUserAgent($this->userAgent)
			->setBaseUrl($this->getApiBaseUrl());

		$request->getCurlOptions()->set(CURLOPT_TIMEOUT, $this->getTimeout());
		$request->addHeaders(array(
			'X-StorageApi-Token' => $this->token,
			'Accept-Encoding' => 'gzip',
		));

		if ($this->getRunId()) {
			$request->addHeader('X-KBC-RunId', $this->getRunId());
		}

		$responseFile = null;
		if ($responseFileName) {
			$responseFile = fopen($responseFileName, "w");
			if (!$responseFile) {
				throw new ClientException("Cannot open file {$responseFileName}");
			}
			$request->setResponseBody($responseFile);
		}

		try {
			$response = $request->send();
		} catch (BadResponseException $e) {
			$response = $e->getResponse();
			$body = $response->json();

			if ($response->getStatusCode() == 503) {
				throw new MaintenanceException(isset($body["reason"]) ? $body['reason'] : 'Maintenance', (string) $response->getHeader('Retry-After'), $body);
			}

			throw new ClientException(
				isset($body['error']) ? $body['error'] : $e->getMessage(),
				$e->getResponse()->getStatusCode(),
				$e,
				isset($body['code']) ? $body['code'] : "",
				$body
			);
		} catch (CurlException $e) {
			throw new ClientException("Http error: " . $e->getMessage(), null, $e, "HTTP_ERROR");
		}

		// wait for asynchronous task completion
		if ($response->getStatusCode() == 202) {
			return $this->handleAsyncTask($response);
		}

		if ($responseFile) {
			fclose($responseFile);
			return "";
		}

		if ($response->getContentType() == 'application/json') {
			return $response->json();
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
		$job = $jobCreatedResponse->json();
		$maxEndTime = time() + $this->getTimeout();
		$maxWaitPeriod = 20;
		$retries = 0;

		// poll for status
		do {
			if (time() >= $maxEndTime) {
				throw new ClientException(
					"Job {$job['id']} execution timeout after " . round($this->getTimeout() / 60) . " minutes."
				);
			}

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
	 *
	 * Prepare URL and call a POST request
	 *
	 * @param string $url
	 * @param array $postData
	 * @return mixed|string
	 */
	public function apiPost($url, $postData=null)
	{
		return $this->request($this->client->post($url, null, $postData));
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
		$request = $this->client->put($url, null, $postData);
		$request->addHeader('content-type', 'application/x-www-form-urlencoded');
		return $this->request($request);
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
		return $this->request($this->client->delete($url));
	}


	/**
	 * @param string $message Message to log
	 * @param array $data Data to log
	 *
	 */
	public function log($message, $data=array())
	{
		if (Client::$log) {
			$data["token"] = $this->getLogData();
			$message = "Storage API: " . $message;
			call_user_func(Client::$log, $message, $data);
		}
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
	 * @static
	 * @param callback $function anonymous function with $message and $data params
	 */
	public static function setLogger($function)
	{
		Client::$log = $function;
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

		while ($parsedLine = fgetcsv($tmpFile, null, ",", '"', '"')) {
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
	 * @param $backoffMaxTries
	 * @return $this
	 */
	public function setBackoffMaxTries($backoffMaxTries)
	{
		$this->backoffMaxTries = (int) $backoffMaxTries;
		return $this;
	}

	/**
	 *
	 * Returns components from indexAction
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
