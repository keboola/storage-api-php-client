<?
namespace Keboola\StorageApi;
class Client
{
	// Stage names
	const STAGE_IN = "in";
	const STAGE_OUT = "out";

	const PARTIAL_UPDATE = true;
	const INCREMENTAL_UPDATE = true;

	// Object token
	public $token;

	// API URL
	private $_apiUrl = "https://connection.keboola.com";

	// Log anonymous function
	private static $_log;

	/**
	 * @param $tokenString
	 * @param $url API Url
	 */
	public function __construct($tokenString, $url=null)
	{
		if ($url) {
			$this->setApiUrl($url);
		}

		$this->token = $this->verifyToken($tokenString);
	}

	/**
	 *
	 * Change API Url
	 *
	 * @param $url
	 */
	public function setApiUrl($url)
	{
		$this->_apiUrl = $url;
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
	 * @param $name
	 * @param $stage
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
	 * @param $bucketId
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
	 * @param $name bucket name
	 * @param $stage bucket stage
	 * @param $description bucket description
	 *
	 * @return string bucket Id
	 */
	public function createBucket($name, $stage, $description)
	{
		$options = array(
			"name" => $name,
			"stage" => $stage,
			"description" => $description,
			"token" => $this->token["token"]
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
	 * @param $bucketId
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
	 * @param $bucketId string
	 * @param $key string
	 * @param $value string
	 */
	public function setBucketAttribute($bucketId, $key, $value)
	{
		$this->_apiPost("/storage/buckets/$bucketId/attributes/$key", array(
			"value" => $value,
		));
	}

	/**
	 *
	 * Delete a bucket attribute
	 *
	 * @param $bucketId
	 * @param $key
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
	 * @param $bucketId
	 * @return bool
	 */
	public function bucketExists($bucketId)
	{
		$buckets = $this->listBuckets();
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
	 * @param $bucketId
	 * @param $name
	 * @param $dataFile string Oneliner with table headers
	 * @param $delimiter string
	 * @param $enclosure string
	 * @param $primaryKey string
	 * @param bool|int $transactional bool
	 * @return mixed
	 */
	public function createTable($bucketId, $name, $dataFile, $delimiter=",", $enclosure='"', $primaryKey=null,
								$transactional=0)
	{
		$options = array(
			"token" => $this->token["token"],
			"bucketId" => $bucketId,
			"name" => $name,
			"delimiter" => $delimiter,
			"enclosure" => $enclosure,
			"primaryKey" => $primaryKey,
			"transactional" => $transactional,
			"data" => "@" . $dataFile
		);

		$tableId = $this->getTableId($name, $bucketId);
		if ($tableId) {
			return $tableId;
		}
		$result = $this->_apiPost("/storage/buckets/" . $bucketId . "/tables", $options);

		$this->_log("Table {$result["id"]} created", array("options" => $options, "result" => $result));

		return $result["id"];

	}

	/**
	 * Create table alias
	 * @param $bucketId
	 * @param $sourceTableId
	 * @param $name string
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
	 * @param $bucketId string limit search to a specific bucket
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
	 * @param $name
	 * @param $bucketId
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
	 * @param $tableId
	 * @param $dataFile
	 * @param $transaction string
	 * @param $delimiter string
	 * @param $enclosure string
	 * @param bool $incremental
	 * @param bool $partial
	 * @return mixed|string
	 */
	public function writeTable($tableId, $dataFile, $transaction=null, $delimiter=",", $enclosure='"',
							   $incremental=false, $partial=false)
	{
		// TODO Gzip data
		$options = array(
			"token" => $this->token["token"],
			"tableId" => $tableId,
			"delimiter" => $delimiter,
			"enclosure" => $enclosure,
			"transaction" => $transaction,
			"incremental" => $incremental,
			"partial" => $partial,
			"data" => "@" . $dataFile
		);
		$result = $this->_apiPost("/storage/tables/{$tableId}/import" , $options);

		$this->_log("Data written to table {$tableId}", array("options" => $options, "result" => $result));

		return $result;
	}

	/**
	 *
	 * Get table details
	 *
	 * @param $tableId
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
	 * @param $tableId
	 * @return mixed|string
	 */
	public function dropTable($tableId)
	{
		$result = $this->_apiDelete("/storage/tables/" . $tableId);
		$this->_log("Table {$tableId} deleted");
		return $result;
	}

	/**
	 *
	 * Set a table attribute
	 *
	 * @param $tableId string
	 * @param $key string
	 * @param $value string
	 */
	public function setTableAttribute($tableId, $key, $value)
	{
		$this->_apiPost("/storage/tables/$tableId/attributes/$key", array(
			"value" => $value,
		));
	}

	/**
	 *
	 * Delete a table attribute
	 *
	 * @param $tableId
	 * @param $key
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
	 * Checks if a table exists
	 *
	 * @param $tableId
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
	 * @param $tableId string Storage API table id
	 * @param $tableName string target table name (optional)
	 * @return string
	 */
	public function getTableDefinition($tableId, $tableName=null)
	{
		if (!$tableName) {
			$tableName =  $tableId;
		}
		$table = $this->getTable($tableId);
		$definition = "CREATE TABLE `{$tableName}`\n(";

		// Column definition
		$columns = array();
		foreach($table["columns"] as $column) {
			if (in_array($column, $table["primaryKey"])) {
				$columns[] = "`{$column}` VARCHAR(255) NOT NULL DEFAULT ''";
			} else {
				$columns[] = "`{$column}` TEXT NOT NULL";
			}
		}
		$definition .= join(",\n", $columns);

		// Primary key indexes
		if ($table["primaryKey"] && count($table["primaryKey"])) {
			$definition .= ",\n PRIMARY KEY (`" . join("`, `", $table["primaryKey"]) . "`)";
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
	 * @param $tokenId string token id
	 * @return mixed|string
	 */
	public function getToken($tokenId)
	{
		return $this->_apiGet("/storage/tokens/" . $tokenId);
	}

	/**
	 *
	 * Verify the token
	 *
	 * @param $token string Optional token
	 * @return mixed|string
	 */
	public function verifyToken($token=null)
	{
		if (!$token) {
			$token = $this->token["token"];
		}
		$tokenObj = $this->_apiGet("/storage/tokens/verify", $token);

		$this->_log("Token verified", array("token" => $tokenObj));

		return $tokenObj;
	}

	/**
	 *
	 * create a new token
	 *
	 * @param $permissions array hash bucketId => permission (read/write)
	 * @param $description string
	 * @return integer token id
	 */
	public function createToken($permissions, $description=null)
	{
		$options = array();
		foreach($permissions as $tableId => $permission) {
			$key = "bucketPermissions[{$tableId}]";
			$options[$key] = $permission;
		}
		if ($description) {
			$options["description"] = $description;
		}

		$result = $this->_apiPost("/storage/tokens", $options);

		$this->_log("Token {$result["id"]} created", array("options" => $options, "result" => $result));

		return $result["id"];
	}

	/**
	 *
	 * update token details
	 *
	 * @param $tokenId string
	 * @param $permissions array
	 * @param $description string
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
	 * @param $tokenId
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
	 * @param $tokenId string If not set, defaults to self
	 * @return string new token
	 */
	public function refreshToken($tokenId=null)
	{
		if ($tokenId == null) {
			$tokenId = $this->token["id"];
		}

		$result = $this->_apiPost("/storage/tokens/" . $tokenId . "/refresh");

		if ($this->token["id"] == $result["id"]) {
			$this->token = $result;
		}

		$this->_log("Token {$tokenId} refreshed", array("token" => $result));

		return $result["token"];
	}

	/**
	 *
	 * Generate GoodData XML configuration for table
	 * TODO Test!
	 *
	 * @param $tableId
	 * @param $fileName string file to store data
	 * @return mixed|string
	 */
	public function getGdXmlConfig($tableId, $fileName=null)
	{
		return $this->_apiGet("/storage/tables/{$tableId}/gooddata-xml", null, $fileName);
	}

	/**
	 *
	 * Exports table contents to CSV
	 *
	 * @param $tableId string
	 * @param $fileName string file to store data
	 * @param $limit int TODO to be implemented
	 * @param $days  int TODO to be implemented
	 * @return string data
	 */
	public function exportTable($tableId, $fileName=null, $limit=0, $days=0)
	{
		return $this->_apiGet("/storage/tables/{$tableId}/export", null, $fileName);
	}

	/**
	 *
	 * Uploads a file
	 *
	 * TODO Test!
	 *
	 * @param $fileName
	 * @return mixed|string
	 */
	public function uploadFile($fileName)
	{
		// TODO Gzip data
		$options = array(
			"token" => $this->token["token"],
			"file" => "@" . $fileName
		);

		$result = $this->_apiPost("/storage/files/", $options);

		$this->_log("File {$fileName} uploaded ", array("options" => $options, "result" => $result));

		return true;
	}

	/**
	 *
	 * Generates URL for api call
	 *
	 * @param $url string
	 * @param $token string Optional token
	 * @return string
	 */
	private function _constructUrl($url, $token=null)
	{
		if (!$token) {
			$token = $this->token["token"];
		}
		return $this->_apiUrl . $url . "?token=" . $token;
	}

	/**
	 *
	 * Converts JSON to object and detects errors
	 *
	 * @param $jsonString
	 * @throws ClientException
	 * @return mixed
	 */
	private function _parseResponse($jsonString)
	{
		$data = json_decode($jsonString, true);
		if ($data === null) {
			return null;
		}
		if (is_string($data)) {
			return $data;
		}
		if(isset($data["error"])) {
			throw new ClientException($data["error"]);
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
	 * @param $url
	 * @param $token
	 * @param null $fileName
	 * @return mixed|string
	 */
	protected function _apiGet($url, $token=null, $fileName=null)
	{
		return $this->_curlGet($this->_constructUrl($url, $token), $fileName);
	}

	/**
	 *
	 * Prepare URL and call a POST request
	 *
	 * @param $url
	 * @param $postData
	 * @param $token
	 * @return mixed|string
	 */
	protected function _apiPost($url, $postData=null, $token=null)
	{
		return $this->_curlPost($this->_constructUrl($url, $token), $postData);
	}

	/**
	 *
	 * Prepare URL and call a POST request
	 *
	 * @param $url
	 * @param $postData
	 * @param $token
	 * @return mixed|string
	 */
	protected function _apiPut($url, $postData=null, $token=null)
	{
		return $this->_curlPut($this->_constructUrl($url, $token), $postData);
	}

	/**
	 *
	 * Prepare URL and call a DELETE request
	 *
	 * @param $url
	 * @param $token
	 * @return mixed|string
	 */
	protected function _apiDelete($url, $token=null)
	{
		return $this->_curlDelete($this->_constructUrl($url, $token));
	}

	/**
	 *
	 * CURL GET request, may be written to a file
	 *
	 * @param $url
	 * @param $fileName
	 * @return bool|mixed|string
	 * @throws ClientException
	 */
	protected function _curlGet($url, $fileName=null)
	{

		$logData = array("url" => $url);
		Client::_timer("request");

		$ch = $this->_curlSetOpts();
		curl_setopt($ch, CURLOPT_URL, $url);

		if ($fileName) {
			$file = fopen($fileName, "w");
			if (!$file) {
				throw new ClientException("Cannot open file {$fileName}");
			}
			curl_setopt($ch, CURLOPT_FILE, $file);
		}

		$result = curl_exec($ch);
		curl_close($ch);

		if ($fileName) {
			fclose($file);
			// Read the first line from the file, as it might contain errors
			$file = fopen($fileName, "r");
			$result = fgets($file);
			fclose($file);
		}

		$logData["requestTime"] = Client::_timer("request");

		if ($result) {
			$this->_log("GET Request finished", $logData);
			try {
				$parsedData = $this->_parseResponse($result);
				// If data cannot be parsed, there might be no error - JSON not parsed
				if ($parsedData===null) {
					if ($fileName) {
						return true;
					}
					return $result;
				}
				return $parsedData;
			} catch (ClientException $e) {
				$errData = array(
					"error" => $e->getMessage(),
					"url" => $url
				);
				$this->_log("Error in GET request response", $errData);
				throw $e;
			}
		} else {
			$curlError = curl_error($ch);
			$logData["curlError"] = $curlError;
			$this->_log("GET Request failed", $logData);
			throw new ClientException("CURL: " . curl_error($ch));
		}
	}

	/**
	 *
	 * CURL POST request
	 *
	 * @param $url
	 * @param $postData array
	 * @throws ClientException
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
		curl_close($ch);

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
			$curlError = curl_error($ch);
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
	 * @param $url
	 * @throws ClientException
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
		$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
		curl_close($ch);

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
			$curlError = curl_error($ch);
			$logData["curlError"] = $curlError;
			$this->_log("POST Request failed", $logData);
			throw new ClientException("CURL: " . $curlError);
		}
	}

	/**
	 *
	 * Init cUrl and set common params
	 *
	 * @return resource
	 */
	protected function _curlSetOpts()
	{
		$ch = curl_init();
		curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_HEADER, false);
		curl_setopt($ch, CURL_HTTP_VERSION_1_1, true);
		curl_setopt($ch, CURLOPT_HTTPGET, true);
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
		return $ch;
	}

	/**
	 * @param $message string Messageto log
	 * @param $data array Data to log
	 *
	 */
	protected function _log($message, $data=array())
	{
		if (Client::$_log) {
			$data["token"] = $this->token["token"];
			$message = "Storage API: " . $message;
			call_user_func(Client::$_log, $message, $data);
		}
	}

	/**
	 * @static
	 * @param $function function anonymous function with $message and $data params
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
	 * @param $csv
	 * @param $header bool if first line contains header
	 * @param $delimiter string CSV delimiter
	 * @param $enclosure string CSV field enclosure
	 * @param null $escape
	 * @return array
	 */
	public static function parseCsv($csv, $header=true, $delimiter=",", $enclosure='"', $escape=null)
	{
		$data = array();
		$headers = array();
		$firstLine = true;
		foreach(explode("\n", $csv) as $line) {
			if (trim($line) == "") {
				continue;
			}
			$parsedLine = str_getcsv($line, $delimiter, $enclosure, $escape);
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
		return $data;
	}

	/**
	 * Timer function
	 *
	 * @param $name string
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

}