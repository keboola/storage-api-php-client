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
use Guzzle\Http\EntityBody;
use Keboola\Csv\CsvFile,
	Guzzle\Http\Client as GuzzleClient;

class Client
{
	// Stage names
	const STAGE_IN = "in";
	const STAGE_OUT = "out";
	const STAGE_SYS = "sys";

	const PARTIAL_UPDATE = true;
	const INCREMENTAL_UPDATE = true;


	// Token string
	public $token;

	// Token object
	private $_tokenObj = null;

	// curren run id sent with all request
	private $_runId = null;

	// API URL
	private $_apiUrl = "https://connection.keboola.com";

	private $_apiVersion = "v2";

	private $_backoffMaxTries;

	// User agent header send with each API request
	private $_userAgent = 'Keboola Storage API PHP Client';

	// Log anonymous function
	private static $_log;

	/**
	 * @var \Guzzle\Http\Client
	 */
	private $_client;

	/**
	 *
	 * Request timeout in seconds
	 *
	 * @var int
	 */
	public $_connectionTimeout = 1800;

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
		$this->_backoffMaxTries = (int) $backoffMaxTries;
		$this->_initClient();
		$this->_initExponentialBackoff();
		$this->_initLogger();

		$this->verifyToken();
	}

	protected function _initClient()
	{
		$this->_client = new GuzzleClient($this->_getApiBaseUrl(), array(
			'version' => $this->_apiVersion,
			CURLOPT_ENCODING => "gzip"
		));
	}

	protected function _getApiBaseUrl()
	{
		return $this->getApiUrl() . "/{version}";
	}

	protected function _initExponentialBackoff()
	{
		$backoffPlugin = BackoffPlugin::getExponentialBackoff(
			$this->_backoffMaxTries,
			array(500,  503),
			array(
				CURLE_COULDNT_RESOLVE_HOST, CURLE_COULDNT_CONNECT, CURLE_WRITE_ERROR, CURLE_READ_ERROR,
				CURLE_OPERATION_TIMEOUTED, CURLE_SSL_CONNECT_ERROR, CURLE_HTTP_PORT_FAILED, CURLE_GOT_NOTHING,
				CURLE_SEND_ERROR, CURLE_RECV_ERROR, CURLE_PARTIAL_FILE
			)
		);
		$backoffPlugin->setEventDispatcher($this->_client->getEventDispatcher());
		$this->_client->addSubscriber($backoffPlugin);
	}


	protected function _initLogger()
	{
		$sapiClient = $this;
		$logAdapter = new ClosureLogAdapter(function($message, $priority, $extras) use ($sapiClient) {
			$params = array();
			if (isset($extras['response']) && $extras['response'] instanceof Response) {
				$params['duration'] = $extras['response']->getInfo('total_time');
			}
			$sapiClient->_guzzleLog($message, $params);
		});

		$this->_client->addSubscriber(new LogPlugin(
			$logAdapter,
			"HTTP request: [{ts}] \"{method} {resource} {protocol}/{version}\" {code} {res_header_Content-Length}"
		));

		$backoffLogger = new BackoffLogger(
			$logAdapter,
			new MessageFormatter('[{ts}] {method} {url} - {code} {phrase} - Retries: {retries}, Delay: {delay}, cURL: {curl_code} {curl_error}')
		);
		$this->_client->addSubscriber($backoffLogger);
	}

	/**
	 * Call private method from closure hack
	 * @param $method
	 * @param $args
	 * @return mixed
	 * @throws \BadMethodCallException
	 */
	public function __call($method, $args) {

		if ($method == '_guzzleLog') {
			return call_user_func_array(array($this, '_log'), $args);
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
		$this->_apiUrl = $url;
	}

	/**
	 * Get API Url
	 *
	 * @return string
	 */
	public function getApiUrl()
	{
		return $this->_apiUrl;
	}


	/**
	 * @param string $userAgent
	 * @return Client
	 */
	public function setUserAgent($userAgent)
	{
		$this->_userAgent = (string) $userAgent;
		return $this;
	}

	public function indexAction()
	{
		return $this->_apiGet("storage");
	}

	/**
	 * Get UserAgent name
	 *
	 * @return string
	 */
	public function getUserAgent()
	{
		return $this->_userAgent;
	}

	/**
	 *
	 * List all buckets
	 *
	 * @return mixed|string
	 */
	public function listBuckets($options = array())
	{
		return $this->_apiGet("storage/buckets?" . http_build_query($options));
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
		return $this->_apiGet("storage/buckets/" . $bucketId);
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

		$result = $this->_apiPost("storage/buckets", $options);

		$this->_log("Bucket {$result["id"]} created", array("options" => $options, "result" => $result));

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
		return $this->_apiDelete("storage/buckets/" . $bucketId);
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
		$this->_apiPost("storage/buckets/$bucketId/attributes/$key", $data);
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
		$result = $this->_apiDelete("storage/buckets/$bucketId/attributes/$key");
		$this->_log("Bucket $bucketId attribute $key deleted");
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
		$buckets = $this->listBuckets();
		if (!count($buckets) || !is_array($buckets)) {
			return false;
		}
		foreach($buckets as $bucket)
		{
			if ($bucket["id"] == $bucketId) {
				return true;
			}
		}
		return false;
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

		if ($this->_isUrl($csvFile->getPathname())) {
			$options["dataUrl"] = $csvFile->getPathname();
		} else {
			$options["data"] = "@{$csvFile->getPathname()}";
		}

		$tableId = $this->getTableId($name, $bucketId);
		if ($tableId) {
			return $tableId;
		}
		$result = $this->_apiPost("storage/buckets/" . $bucketId . "/tables", $options);

		$this->_log("Table {$result["id"]} created", array("options" => $options, "result" => $result));

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

		if ($this->_isUrl($csvFile->getPathname())) {
			$options['dataUrl'] = $csvFile->getPathname();
		} else {
			// upload file
			$fileId = $this->uploadFile($csvFile->getPathname(), false, false);
			$options['dataFileId'] = $fileId;
		}

		$createdTable = $this->_apiPost("storage/buckets/{$bucketId}/tables-async", $options);

		return $createdTable['id'];
	}

	/**
	 * @param $bucketId destination bucket
	 * @param $snapshotId source snapshot
	 * @param null $name table name (optional) otherwise fetched from snapshot
	 */
	public function createTableFromSnapshot($bucketId, $snapshotId, $name = null)
	{
		$createdTable = $this->_apiPost("storage/buckets/{$bucketId}/tables-async", array(
			'snapshotId' => $snapshotId,
			'name' => $name,
		));
		return $createdTable['id'];
	}

	private function _isUrl($path)
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

		$result = $this->_apiPost("storage/buckets/" . $bucketId . "/table-aliases", $filteredOptions);
		$this->_log("Table alias {$result["id"]}  created", array("options" => $filteredOptions, "result" => $result));
		return $result["id"];
	}

	/**
	 * @param $tableId
	 * @return mixed
	 */
	public function createTableSnapshot($tableId, $snapshotDescription = null)
	{
		$result = $this->_apiPost("storage/tables/{$tableId}/snapshots", array(
			'description' => $snapshotDescription,
		));
		$this->_log("Snapthos {$result['id']} of table {$tableId} created.");
		return $result["id"];
	}

	public function rollbackTableFromSnapshot($tableId, $snapshotId)
	{
		return $this->_apiPost("storage/tables/{$tableId}/rollback", array(
			'snapshotId' => $snapshotId,
		));
	}

	/**
	 * @param $tableId
	 * @return mixed|string
	 */
	public function listTableSnapshots($tableId, $options = array())
	{
		return $this->_apiGet("storage/tables/{$tableId}/snapshots?" . http_build_query($options));
	}

	/**
	 * @param $tableId
	 * @param array $filter
	 * @return mixed|string
	 */
	public function setAliasTableFilter($tableId, array $filter)
	{
		$result = $this->_apiPost("storage/tables/$tableId/alias-filter", $filter);
		$this->_log("Table $tableId  filter set", array(
			'filter' => $filter,
			'result' => $result,
		));
		return $result;
	}

	public function removeAliasTableFilter($tableId)
	{
		$this->_apiDelete("storage/tables/$tableId/alias-filter");
	}

	/**
	 * @param $tableId
	 */
	public function enableAliasTableColumnsAutoSync($tableId)
	{
		$this->_apiPost("storage/tables/{$tableId}/alias-columns-auto-sync");
	}

	/**
	 * @param $tableId
	 */
	public function disableAliasTableColumnsAutoSync($tableId)
	{
		$this->_apiDelete("storage/tables/{$tableId}/alias-columns-auto-sync");
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
			return $this->_apiGet("storage/buckets/{$bucketId}/tables?" . http_build_query($options));
		}
		return $this->_apiGet("storage/tables?" . http_build_query($options));
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
		$options = $this->_writeTableOptionsPrepare($csvFile, $options);

		if ($this->_isUrl($csvFile->getPathname())) {
			$options["dataUrl"] = $csvFile->getPathname();
		} else {
			$options["data"] = "@{$csvFile->getRealPath()}";
		}

		$result = $this->_apiPost("storage/tables/{$tableId}/import" , $options);

		$this->_log("Data written to table {$tableId}", array("options" => $options, "result" => $result));

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
		$options = $this->_writeTableOptionsPrepare($csvFile, $options);

		if ($this->_isUrl($csvFile->getPathname())) {
			$options['dataUrl'] = $csvFile->getPathname();
		} else {
			// upload file
			$fileId = $this->uploadFile($csvFile->getPathname(), false, false);
			$options['dataFileId'] = $fileId;
		}

		return $this->_apiPost("storage/tables/{$tableId}/import-async", $options);
	}


	private function _writeTableOptionsPrepare(CsvFile $csvFile, $options)
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
		return $this->_apiGet("storage/tables/" . $tableId);
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
		$result = $this->_apiDelete("storage/tables/" . $tableId);
		$this->_log("Table {$tableId} deleted");
		return $result;
	}

	/**
	 * Unlink aliased table from source table
	 * @param string $tableId
	 * @return mixed|string
	 */
	public function unlinkTable($tableId)
	{
		$result = $this->_apiDelete("storage/tables/" . $tableId . '?unlink');
		$this->_log("Table {$tableId} unlinked");
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
		$this->_apiPost("storage/tables/$tableId/attributes/$key", $data);
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
		$result = $this->_apiDelete("storage/tables/$tableId/attributes/$key");
		$this->_log("Table $tableId attribute $key deleted");
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
		$this->_apiPost("storage/tables/$tableId/columns", $data);
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
		$this->_apiDelete("storage/tables/$tableId/columns/$name");
		$this->_log("Table $tableId column $name deleted");
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
		$this->_apiPost("storage/tables/$tableId/indexed-columns", $data);
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
		$this->_apiDelete("storage/tables/$tableId/indexed-columns/$columnName");
		$this->_log("Table $tableId indexed column $columnName deleted");
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
		$tables = $this->listTables();
		foreach($tables as $table)
		{
			if ($table["id"] == $tableId) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @param $jobId
	 * @return mixed|string
	 */
	public function getJob($jobId)
	{
		return $this->_apiGet("storage/jobs/" . $jobId);
	}

	/**
	 *
	 * returns all tokens
	 *
	 * @return mixed|string
	 */
	public function listTokens()
	{
		return $this->_apiGet("storage/tokens");
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
		return $this->_apiGet("storage/tokens/" . $tokenId);
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
		$tokenObj = $this->_apiGet("storage/tokens/verify");

		$this->_tokenObj = $tokenObj;
		$this->_log("Token verified");

		return $tokenObj;
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

		$result = $this->_apiPost("storage/tokens", $options);

		$this->_log("Token {$result["id"]} created", array("options" => $options, "result" => $result));

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

		$result = $this->_apiPut("storage/tokens/" . $tokenId, http_build_query($options));

		$this->_log("Token {$tokenId} updated", array("options" => $options, "result" => $result));

		return $tokenId;
	}

	/**
	 * @param string $tokenId
	 * @return mixed|string
	 */
	public function dropToken($tokenId)
	{
		$result = $this->_apiDelete("storage/tokens/" . $tokenId);
		$this->_log("Token {$tokenId} deleted");
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

		$result = $this->_apiPost("storage/tokens/" . $tokenId . "/refresh");

		if ($currentToken["id"] == $result["id"]) {
			$this->token = $result['token'];
			$this->_tokenObj = $result;
		}

		$this->_log("Token {$tokenId} refreshed", array("token" => $result));

		return $result["token"];
	}

	/**
	 * @param $tokenId
	 * @param $recipientEmail
	 * @param $message
	 */
	public function shareToken($tokenId, $recipientEmail, $message)
	{
		$this->_apiPost("storage/tokens/$tokenId/share", array(
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
		return $this->_apiGet("storage/tables/{$tableId}/gooddata-xml", null, $fileName);
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

		$allowedOptions = array(
			'limit',
			'changedSince',
			'changedUntil',
			'escape',
			'format',
			'whereColumn',
			'whereOperator'
		);

		$filteredOptions = array_intersect_key($options, array_flip($allowedOptions));

		if (isset($options['columns'])) {
			$filteredOptions['columns'] = implode(',', (array) $options['columns']);
		}

		if (isset($options['whereValues'])) {
			$filteredOptions['whereValues'] = (array) $options['whereValues'];
		}

		$url .= '?' . http_build_query($filteredOptions);

		return $this->_apiGet($url, $fileName);
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

		return $this->_apiDelete($url);
	}

	/**
	 *
	 * Uploads a file
	 *
	 *
	 * @param string $fileName
	 * @param bool $isPublic
	 * @return mixed|string
	 */
	public function uploadFile($fileName, $isPublic = false, $notify = true)
	{
		// 1. prepare resource
		$result = $this->_apiPost("storage/files/prepare", array(
			'isPublic' => $isPublic,
			'notify' => $notify,
			'name' => basename($fileName),
			'sizeBytes' => filesize($fileName),
		));

		// 2. upload directly do S3 using returned credentials
		$uploadParams = $result['uploadParams'];
		$curlopts = array(
			CURLOPT_ENCODING => "gzip"
		);
		$client = new GuzzleClient($uploadParams['url'], array('curl.options' => $curlopts));
		$client->addSubscriber(BackoffPlugin::getExponentialBackoff());

		try {
			$body = EntityBody::factory(array(
				'key' => $uploadParams['key'],
				'acl' => $uploadParams['acl'],
				'signature' => $uploadParams['signature'],
				'policy' => $uploadParams['policy'],
				'AWSAccessKeyId' => $uploadParams['AWSAccessKeyId'],
				'file' => "@$fileName",
			));
			$body->compress();
			$client->post('/', null, $body)->send();
		} catch(RequestException $e) {
			throw new ClientException("Error on file upload to S3: " . $e->getMessage(), $e->getCode(), $e);
		}

		return $result['id'];
	}

	/**
	 * Get a single file
	 * @param string $fileId
	 * @return mixed|string
	 */
	public function getFile($fileId)
	{
		return $this->_apiGet('storage/files/' . $fileId);
	}

	/**
	 * Files list
	 */
	public function listFiles()
	{
		return $this->_apiGet('storage/files');
	}


	/**
	 * Create new event
	 * @param Event $event
	 * @return mixed|string
	 */
	public function createEvent(Event $event)
	{
		$result = $this->_apiPost('storage/events', array(
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
		return $this->_apiGet('storage/events/' . $id);
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
		return $this->_apiGet('storage/events?' . http_build_query($queryParams));
	}

	public function listTableEvents($tableId, $params = array())
	{
		$defaultParams = array(
			'limit' => 100,
			'offset' => 0,
		);

		$queryParams = array_merge($defaultParams, $params);
		return $this->_apiGet("storage/tables/{$tableId}/events?" . http_build_query($queryParams));
	}

	public function getSnapshot($id)
	{
		return $this->_apiGet("storage/snapshots/$id");
	}

	/**
	 * Unique 64bit sequence generator
	 * @return mixed
	 */
	public function generateId()
	{
		$result = $this->_apiPost('storage/tickets');
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
	protected function _apiGet($url, $fileName=null)
	{
		return $this->_request($this->_client->get($url), $fileName);
	}

	protected function _request(RequestInterface $request, $responseFileName = null )
	{
		$this->_client
			->setUserAgent($this->_userAgent)
			->setBaseUrl($this->_getApiBaseUrl());

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
			return $this->_handleAsyncTask($response);
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
	protected function _handleAsyncTask(Response $jobCreatedResponse)
	{
		$job = $jobCreatedResponse->json();
		$maxEndTime = time() + $this->getTimeout();
		$maxWaitPeriod = 60;
		$retries = 0;

		// poll for status
		do {
			$job = $this->getJob($job['id']);

			if (time() >= $maxEndTime) {
				throw new ClientException(
					"Job {$job['id']} execution timeout after " . round($this->getTimeout() / 60) . " minutes."
				);
			}

			$waitSeconds = min(pow(2, $retries), $maxWaitPeriod);
			sleep($waitSeconds);
			$retries++;
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
	protected function _apiPost($url, $postData=null)
	{
		return $this->_request($this->_client->post($url, null, $postData));
	}

	/**
	 *
	 * Prepare URL and call a POST request
	 *
	 * @param string $url
	 * @param array $postData
	 * @return mixed|string
	 */
	protected function _apiPut($url, $postData=null)
	{
		$request = $this->_client->put($url, null, $postData);
		$request->addHeader('content-type', 'application/x-www-form-urlencoded');
		return $this->_request($request);
	}

	/**
	 *
	 * Prepare URL and call a DELETE request
	 *
	 * @param string $url
	 * @return mixed|string
	 */
	protected function _apiDelete($url)
	{
		return $this->_request($this->_client->delete($url));
	}


	/**
	 * @param string $message Message to log
	 * @param array $data Data to log
	 *
	 */
	protected function _log($message, $data=array())
	{
		if (Client::$_log) {
			$data["token"] = $this->getLogData();
			$message = "Storage API: " . $message;
			call_user_func(Client::$_log, $message, $data);
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
		if (!$this->_tokenObj) {
			$logData["token"] = substr($this->token, 0, 6);
			return $logData;
		}
		$logData = array();
		$logData["token"] = substr($this->_tokenObj["token"], 0, 6);
		$logData["owner"] = $this->_tokenObj["owner"];
		$logData["id"] = $this->_tokenObj["id"];
		$logData["description"] = $this->_tokenObj["description"];
		$logData["url"] = $this->_apiUrl;
		return $logData;

	}

	/**
	 * @static
	 * @param callback $function anonymous function with $message and $data params
	 */
	public static function setLogger($function)
	{
		Client::$_log = $function;
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
		$this->_connectionTimeout = $timeout;
	}

	/**
	 *
	 * Get CURL timeout in seconds
	 *
	 * @return int
	 */
	public function getTimeout()
	{
		return $this->_connectionTimeout;
	}

	public function getRunId()
	{
		return $this->_runId;
	}

	/**
	 * @param $runId
	 * @return Client
	 */
	public function setRunId($runId)
	{
		$this->_runId = $runId;
		return $this;
	}

	/**
	 * @return int
	 */
	public function getBackoffMaxTries()
	{
		return $this->_backoffMaxTries;
	}

	/**
	 * @param $backoffMaxTries
	 * @return $this
	 */
	public function setBackoffMaxTries($backoffMaxTries)
	{
		$this->_backoffMaxTries = (int) $backoffMaxTries;
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
