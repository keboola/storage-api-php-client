<?php
namespace Keboola\StorageApi;

class Client
{
	// Stage names
	const STAGE_IN = "in";
	const STAGE_OUT = "out";
	const STAGE_SYS = "sys";

	const PARTIAL_UPDATE = true;
	const INCREMENTAL_UPDATE = true;

	// Throw an Exception if Storage API returns an error
	// If false, just return the error response
	public $translateApiErrors = true;

	// Token string
	public $token;

	// Token object
	private $_tokenObj = null;

	// curren run id sent with all request
	private $_runId = null;

	// API URL
	private $_apiUrl = "https://connection.keboola.com";

	private $_apiVersion = "v2";

	// User agent header send with each API request
	private $_userAgent = 'Keboola Storage API PHP Client';

	// Log anonymous function
	private static $_log;

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
	public function __construct($tokenString, $url=null, $userAgent=null)
	{
		if ($url) {
			$this->setApiUrl($url);
		}

		if ($userAgent) {
			$this->setUserAgent($userAgent);
		}

		$this->token = $tokenString;
		$this->verifyToken();
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
	 * @param string $userAgent
	 * @return Client
	 */
	public function setUserAgent($userAgent)
	{
		$this->_userAgent = (string) $userAgent;
		return $this;
	}

	/**
	 *
	 * List all buckets
	 *
	 * @return mixed|string
	 */
	public function listBuckets()
	{
		return $this->_apiGet("/storage/buckets");
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
		return $this->_apiGet("/storage/buckets/" . $bucketId);
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

		$result = $this->_apiPost("/storage/buckets", $options);

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
		return $this->_apiDelete("/storage/buckets/" . $bucketId);
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
		$this->_apiPost("/storage/buckets/$bucketId/attributes/$key", $data);
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
		$result = $this->_apiDelete("/storage/buckets/$bucketId/attributes/$key");
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
	 *
	 * Creates a table and returns table id. If table exists, returns table id.
	 *
	 * @param string $bucketId
	 * @param string $name
	 * @param string $dataFile local file or url
	 * @param string $delimiter
	 * @param string $enclosure
	 * @param string null $primaryKey
	 * @param bool|int $transactional
	 * @param string null $transaction
	 * @return mixed
	 */
	public function createTable($bucketId, $name, $dataFile, $delimiter=",", $enclosure='"', $primaryKey=null,
								$transactional=0, $transaction=null)
	{
		$options = array(
			"bucketId" => $bucketId,
			"name" => $name,
			"delimiter" => $delimiter,
			"enclosure" => $enclosure,
			"primaryKey" => $primaryKey,
			"transactional" => $transactional,
		);
		if ($transaction) {
			$options["transaction"] = $transaction;
		}

		if ($this->_isUrl($dataFile)) {
			$options["dataUrl"] = $dataFile;
		} else {
			$options["data"] = "@$dataFile";
		}

		$tableId = $this->getTableId($name, $bucketId);
		if ($tableId) {
			return $tableId;
		}
		$result = $this->_apiPost("/storage/buckets/" . $bucketId . "/tables", $options);

		$this->_log("Table {$result["id"]} created", array("options" => $options, "result" => $result));

		return $result["id"];

	}

	private function _isUrl($path)
	{
		return preg_match('/^https?:\/\/.*$/', $path);
	}

	/**
	 * Create table alias
	 * @param string $bucketId
	 * @param string $sourceTableId
	 * @param string null $name
	 * @return mixed
	 */
	public function createAliasTable($bucketId, $sourceTableId, $name=NULL)
	{
		$options = array(
			'sourceTable' => $sourceTableId,
			'name' => $name,
		);

		$result = $this->_apiPost("/storage/buckets/" . $bucketId . "/table-aliases", $options);
		$this->_log("Table alias {$result["id"]}  created", array("options" => $options, "result" => $result));
		return $result["id"];
	}

	/**
	 *
	 * Get all available tables
	 *
	 * @param string $bucketId limit search to a specific bucket
	 * @return mixed|string
	 */
	public function listTables($bucketId=null)
	{
		if ($bucketId) {
			return $this->_apiGet("/storage/buckets/{$bucketId}/tables");
		}
		return $this->_apiGet("/storage/tables");
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
	 *
	 * Writes data to table
	 *
	 * @param string $tableId
	 * @param string $dataFile local path to file or file URL
	 * @param string null $transaction
	 * @param string $delimiter
	 * @param string $enclosure
	 * @param bool $incremental
	 * @param bool $partial
	 * @return mixed|string
	 */
	public function writeTable($tableId, $dataFile, $transaction=null, $delimiter=",", $enclosure='"',
							   $incremental=false, $partial=false)
	{
		// TODO Gzip data
		$options = array(
			"tableId" => $tableId,
			"delimiter" => $delimiter,
			"enclosure" => $enclosure,
			"transaction" => $transaction,
			"incremental" => $incremental,
			"partial" => $partial,
		);

		if ($this->_isUrl($dataFile)) {
			$options["dataUrl"] = $dataFile;
		} else {
			$options["data"] = "@$dataFile";
		}

		$result = $this->_apiPost("/storage/tables/{$tableId}/import" , $options);

		$this->_log("Data written to table {$tableId}", array("options" => $options, "result" => $result));

		return $result;
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
		return $this->_apiGet("/storage/tables/" . $tableId);
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
		$result = $this->_apiDelete("/storage/tables/" . $tableId);
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
		$result = $this->_apiDelete("/storage/tables/" . $tableId . '?unlink');
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
		$this->_apiPost("/storage/tables/$tableId/attributes/$key", $data);
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
		$result = $this->_apiDelete("/storage/tables/$tableId/attributes/$key");
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
		$this->_apiPost("/storage/tables/$tableId/columns", $data);
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
		$this->_apiDelete("/storage/tables/$tableId/columns/$name");
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
		$this->_apiPost("/storage/tables/$tableId/indexed-columns", $data);
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
		$this->_apiDelete("/storage/tables/$tableId/indexed-columns/$columnName");
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
	 *
	 * Generates a MySQL table definition
	 *
	 * @param string $tableId Storage API table id
	 * @param string $tableName target table name (optional)
	 * @param array $options - export options ("columns")
	 * @return string
	 */
	public function getTableDefinition($tableId, $tableName=null, $options=array())
	{
		if (!$tableName) {
			$tableName =  $tableId;
		}
		$table = $this->getTable($tableId);
		$definition = "CREATE TABLE `{$tableName}`\n(";

		// Column definition
		$columns = array();

		// Key length can be 1000 bytes in MySQL
		// As we use UTF-8, every character might occupy up to 3 bytes, thus joint primary keys (varchars)
		// exceed the 1000 byte limit. We calculate maximum varchar length so all columns in the primary key
		// do not exceed the total allowed length.
		$varcharKeyLength = 255;
		if (count($table["primaryKey"]) > 1) {
			$varcharKeyLength = floor (1000 / (count($table["primaryKey"])*3));
		}
		foreach($table["columns"] as $column) {
			if (isset($options["columns"]) && count($options["columns"])) {
				if (!in_array($column, $options["columns"])) {
					continue;
				}
			}
			if (in_array($column, $table["primaryKey"])) {
				$columns[] = "`{$column}` VARCHAR({$varcharKeyLength}) NOT NULL DEFAULT ''";
			} else {
				$columns[] = "`{$column}` TEXT NOT NULL";
			}
		}
		$definition .= join(",\n", $columns);

		// Primary key indexes
		if ($table["primaryKey"] && count($table["primaryKey"])) {
			$includePK = true;
			// Do not create PK if not all parts of the PK are imported
			if (isset($options["columns"]) && count($options["columns"])) {
				foreach($table["primaryKey"] as $pk) {
					if (!in_array($pk, $options["columns"])) {
						$includePK = false;
					}
				}
			}
			if ($includePK) {
				$definition .= ",\n PRIMARY KEY (`" . join("`, `", $table["primaryKey"]) . "`)";
			}
		}
		$definition .= ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
		return $definition;
	}

	/**
	 *
	 * returns all tokens
	 *
	 * @return mixed|string
	 */
	public function listTokens()
	{
		return $this->_apiGet("/storage/tokens");
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
		return $this->_apiGet("/storage/tokens/" . $tokenId);
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
		$tokenObj = $this->_apiGet("/storage/tokens/verify");

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

		$result = $this->_apiPost("/storage/tokens", $options);

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

		$result = $this->_apiPut("/storage/tokens/" . $tokenId, $options);

		$this->_log("Token {$tokenId} updated", array("options" => $options, "result" => $result));

		return $tokenId;
	}

	/**
	 * @param string $tokenId
	 * @return mixed|string
	 */
	public function dropToken($tokenId)
	{
		$result = $this->_apiDelete("/storage/tokens/" . $tokenId);
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

		$result = $this->_apiPost("/storage/tokens/" . $tokenId . "/refresh");

		if ($currentToken["id"] == $result["id"]) {
			$this->token = $result['token'];
			$this->_tokenObj = $result;
		}

		$this->_log("Token {$tokenId} refreshed", array("token" => $result));

		return $result["token"];
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
		return $this->_apiGet("/storage/tables/{$tableId}/gooddata-xml", null, $fileName);
	}

	/**
	 * @param string $tableId
	 * @param string null $fileName
	 * @param array $options - (int) limit, (timestamp | strtotime format) changedSince, (timestamp | strtotime format) changedUntil, (bool) escape, (array) columns
	 * @return mixed|string
	 */
	public function exportTable($tableId, $fileName = null, $options = array())
	{
		$url = "/storage/tables/{$tableId}/export";

		$allowedOptions = array(
			'limit',
			'changedSince',
			'changedUntil',
			'escape',
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
	 *
	 * Uploads a file
	 *
	 *
	 * @param string $fileName
	 * @param bool $isPublic
	 * @return mixed|string
	 */
	public function uploadFile($fileName, $isPublic = false)
	{
		// TODO Gzip data
		$options = array(
			"file" => "@" . $fileName,
			"isPublic" => $isPublic,
		);

		$result = $this->_apiPost("/storage/files/", $options);

		$this->_log("File {$fileName} uploaded ", array("options" => $options, "result" => $result));

		return $result['id'];
	}

	/**
	 * Get a single file
	 * @param string $fileId
	 * @return mixed|string
	 */
	public function getFile($fileId)
	{
		return $this->_apiGet('/storage/files/' . $fileId);
	}

	/**
	 * Files list
	 */
	public function listFiles()
	{
		return $this->_apiGet('/storage/files');
	}


	/**
	 * Create new event
	 * @param Event $event
	 * @return mixed|string
	 */
	public function createEvent(Event $event)
	{
		$result = $this->_apiPost('/storage/events', array(
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
		return $this->_apiGet('/storage/events/' . $id);
	}

	/**
	 * @param int $limit
	 * @param int $offset
	 * @return mixed|string
	 */
	public function listEvents($limit = 100, $offset = 0)
	{
		$queryParams = array(
			'limit' => $limit,
			'offset' => $offset,
		);
		return $this->_apiGet('/storage/events?' . http_build_query($queryParams));
	}

	/**
	 * Unique 64bit sequence generator
	 * @return mixed
	 */
	public function generateId()
	{
		$result = $this->_apiPost('/storage/tickets');
		return $result['id'];
	}

	/**
	 *
	 * Generates URL for api call
	 *
	 * @param string $url
	 * @return string
	 */
	private function _constructUrl($url)
	{
		return $this->_apiUrl . '/' . $this->_apiVersion . $url;
	}

	/**
	 *
	 * Converts JSON to object and detects errors
	 *
	 * @param string $jsonString
	 * @throws ClientException
	 * @return mixed
	 */
	private function _parseResponse($jsonString)
	{
		// Detect JSON string
		if ($jsonString[0] != "{" && $jsonString[0] != "[" ) {
			return null;
		};
		$data = json_decode($jsonString, true);
		if ($data === null) {
			return null;
		}
		if (is_string($data)) {
			return $data;
		}
		if($this->translateApiErrors && isset($data["error"])) {
			$stringCode = null;
			if (isset($data['code'])) {
				$stringCode = $data['code'];
			}
			throw new ClientException($data["error"], null, null, $stringCode);
		}
		if($this->translateApiErrors && isset($data["status"]) && $data["status"] == "maintenance") {
			throw new ClientException($data["reason"], null, null, "MAINTENANCE", $data);
		}
		if (count($data) === 1 && isset($data["uri"])) {
			return $this->_curlGet($data["uri"]);
		}
		return $data;
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
		return $this->_curlGet($this->_constructUrl($url), $fileName);
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
		return $this->_curlPost($this->_constructUrl($url), $postData);
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
		return $this->_curlPut($this->_constructUrl($url), $postData);
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
		return $this->_curlDelete($this->_constructUrl($url));
	}

	/**
	 *
	 * CURL GET request, may be written to a file
	 *
	 * @param string $url
	 * @param string null $fileName
	 * @param bool $gzip
	 * @throws ClientException
	 * @throws Exception
	 * @throws \Exception|ClientException
	 * @return bool|mixed|string
	 */
	protected function _curlGet($url, $fileName=null, $gzip = true)
	{
		$logData = array("url" => $url);
		Client::_timer("request");

		$headers = array();
		$file = null;
		if ($fileName) {

			$file = fopen($fileName, "w");
			if (!$file) {
				throw new ClientException("Cannot open file {$fileName}");
			}
			if ($gzip) {
				$headers[] = "Accept-encoding: gzip";
			}
		}

		$ch = $this->_curlSetOpts($headers);
		curl_setopt($ch, CURLOPT_URL, $url);
		if (is_resource($file)) {
			curl_setopt($ch, CURLOPT_FILE, $file);
		}

		$result = curl_exec($ch);
		$curlError = curl_error($ch);
		$curlErrNo = curl_errno($ch);
		curl_close($ch);

		if ($curlErrNo) {
			throw new Exception($curlError, $curlErrNo, null, "CURL_ERROR");
		}

		if ($fileName) {
			fclose($file);
			// Read the first line from the file, as it might contain errors
			$file = fopen($fileName, "r");
			$result = fgets($file, 1024);
			fclose($file);
		}

		$logData["requestTime"] = Client::_timer("request");

		if (!$result) {
			$logData["curlError"] = $curlError;
			$this->_log("GET Request failed", $logData);
			throw new ClientException("CURL: " . $curlError);
		}

		$this->_log("GET Request finished", $logData);
		try {
			$parsedData = $this->_parseResponse($result);

			// If data cannot be parsed, there might be no error - JSON not parsed
			if ($parsedData !== null) {
				return $parsedData;
			}

			if (!$fileName) {
				return $result;
			}

			if ($gzip) {
				$this->_ungzipFile($fileName);
			}

			return true;
		} catch (ClientException $e) {
			$errData = array(
				"error" => $e->getMessage(),
				"url" => $url
			);
			$this->_log("Error in GET request response", $errData);
			throw $e;
		}
	}

	private function _ungzipFile($fileName)
	{
		$suffix = pathinfo($fileName, PATHINFO_EXTENSION);
		$cmd = 'gzip --d ' . escapeshellarg($fileName) . ' --suffix ' .  '.' . $suffix;

		$expectedFile = pathinfo($fileName, PATHINFO_DIRNAME) . '/' . basename($fileName, '.' . $suffix);
		if (is_file($expectedFile)) {
			throw new  ClientException("Cannot unzip file, file $expectedFile alreadyExists");
		}
		shell_exec($cmd);

		if (!is_file($expectedFile)) {
			throw new ClientException('Error on response decode');
		}

		if (!rename($expectedFile, $fileName)) {
			throw new ClientException('Error on response decode');
		}

	}

	/**
	 *
	 * CURL POST request
	 *
	 * @param string $url
	 * @param array $postData
	 * @param string $method
	 * @throws ClientException
	 * @throws Exception
	 * @throws \Exception|ClientException
	 * @return mixed|string
	 */
	protected function _curlPost($url, $postData=null, $method = 'POST') {

		$logData = array("url" => $url, "postData" => $postData);
		Client::_timer("request");

		$ch = $this->_curlSetOpts();
		curl_setopt($ch, CURLOPT_URL, $url);
		curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
		curl_setopt($ch, CURLOPT_POST, 1);
		if ($postData) {
			curl_setopt($ch, CURLOPT_POSTFIELDS, $postData);
		}
		$result = curl_exec($ch);
		$curlError = curl_error($ch);
		$curlErrNo = curl_errno($ch);
		curl_close($ch);

		if ($curlErrNo) {
			throw new Exception($curlError, $curlErrNo, null, "CURL_ERROR");
		}

		$logData["requestTime"] = Client::_timer("request");

		if ($result) {
			$this->_log("POST Request finished", $logData);
			try{
				return $this->_parseResponse($result);
			} catch (ClientException $e) {
				$errData = array(
					"error" => $e->getMessage(),
					"url" => $url,
					"postData" => $postData
				);
				$this->_log("Error in POST request response", $errData);
				throw $e;
			}
		} else {
			$logData["curlError"] = $curlError;
			$this->_log("POST Request failed", $logData);
			throw new ClientException("CURL: " . $curlError);
		}
	}

	/**
	 *
	 * CURL PUT request
	 *
	 * @param $url
	 * @param $postData array
	 * @throws ClientException
	 * @return mixed|string
	 */
	protected function _curlPut($url, $postData=null)
	{
		$this->_curlPost($url, $postData, 'PUT');
	}

	/**
	 *
	 * CURL DELETE request
	 *
	 * @param string $url
	 * @throws ClientException
	 * @throws Exception
	 * @throws \Exception|ClientException
	 * @return mixed
	 */
	protected function _curlDelete($url)
	{
		$logData = array("url" => $url);
		Client::_timer("request");

		$ch = $this->_curlSetOpts();
		curl_setopt($ch, CURLOPT_URL, $url);
		curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "DELETE");
		$result = curl_exec($ch);
		$curlError = curl_error($ch);
		$curlErrNo = curl_errno($ch);
		$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
		curl_close($ch);

		if ($curlErrNo) {
			throw new Exception($curlError, $curlErrNo, null, "CURL_ERROR");
		}

		$logData["requestTime"] = Client::_timer("request");

		if ($result) {
			$this->_log("DELETE Request finished", $logData);
			try{
				return $this->_parseResponse($result);
			} catch (ClientException $e) {
				$errData = array(
					"error" => $e->getMessage(),
					"url" => $url
				);
				$this->_log("Error in DELETE request response", $errData);
				throw $e;
			}
		} else if ($httpCode == 204) {
			$this->_log("DELETE Request finished", $logData);
			return true;
		} else {
			$logData["curlError"] = $curlError;
			$this->_log("POST Request failed", $logData);
			throw new ClientException("CURL: " . $curlError);
		}
	}

	/**
	 *
	 * Init cUrl and set common params
	 *
	 * @param array $headers
	 * @return resource
	 */
	protected function _curlSetOpts($headers = array())
	{
		if ($this->getRunId()) {
			$headers[] = 'X-KBC-RunId: ' . $this->getRunId();
		}

		$ch = curl_init();
		curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
		curl_setopt($ch, CURLOPT_TIMEOUT, $this->getTimeout());
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_HEADER, false);
		curl_setopt($ch, CURL_HTTP_VERSION_1_1, true);
		curl_setopt($ch, CURLOPT_HTTPGET, true);
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
		curl_setopt($ch, CURLOPT_USERAGENT, $this->_userAgent);
		curl_setopt($ch, CURLOPT_HTTPHEADER, array_merge(array(
			"Connection: close",
			"X-StorageApi-Token: {$this->token}",
		), $headers));
		return $ch;
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
	 * Timer function
	 *
	 * @param string null $name
	 * @return float
	 */
	private function _timer($name=null)
	{
		static $_time = array();
		$now = microtime(true);
		$delta = isset($time[$name]) ? $now-$time[$name] : 0;
		$time[$name] = $now;
		return $delta;
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
}
