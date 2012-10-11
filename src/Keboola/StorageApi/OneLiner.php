<?
namespace Keboola\StorageApi;

use Keboola\Csv\CsvFile;
/**
 *
 * Data model for One Line CSV file
 *
 * Example usage
 *
 * $storageApi = new Keboola\StorageApi\Client($token);
 * Keboola\StorageApi\OneLiner::setClient($storageApi);
 * Keboola\StorageApi\OneLiner::$tmpDir = ROOT_PATH . "/tmp/";
 * $model = new Keboola\StorageApi\OneLiner("in.c-config.SFDC");
 * $model->property = "value";
 * $model–>save();
 *
 */
class OneLiner
{
	/**
	 * @var Client
	 */
	public static $client;

	/**
	 *
	 * Temporary directory
	 *
	 * @var string
	 */
	public static $tmpDir = "/tmp/";

	/**
	 *
	 * Storage for data
	 *
	 * @var array
	 */
	private $_data = array();

	/**
	 *
	 * Storage API table ID
	 *
	 * @var string
	 */
	private $_tableId;

	/**
	 *
	 * Load data from table
	 *
	 * @param $table
	 */
	public function __construct($tableId)
	{
		$this->_tableId = $tableId;
		$this->load();
	}

	/**
	 *
	 * Getter
	 *
	 * @param $name string
	 */
	public function __get($name)
	{
		if (array_key_exists($name, $this->_data)) {
			return $this->_data[$name];
		}
		throw new OneLinerException("Attribute {$name} not found");
	}

	/**
	 *
	 * Setter
	 *
	 * @param $name
	 * @param $value
	 */
	public function __set($name, $value)
	{
		$this->_data[$name] = $value;
	}

	/**
	 * @param $name
	 * @return bool
	 */
	public function __isset($name)
	{
		return isset($this->_data[$name]);
	}

	/**
	 * @param $name
	 */
	public function __unset($name)
	{
		unset($this->_data[$name]);
	}

	/**
	 *
	 * Write table to Storage API, create table if not exists
	 *
	 */
	public function save()
	{
		$dataFile = tempnam(self::$tmpDir, "oneliner");

		$csvFile = new CsvFile($dataFile);
		$csvFile->writeRow(array_keys($this->_data));
		$csvFile->writeRow($this->_data);

		if (!self::$client->tableExists($this->_tableId)) {
			$tableInfo = explode(".",$this->_tableId);
			self::$client->createTable($tableInfo[0] . "." . $tableInfo[1], $tableInfo[2], $csvFile);
		}

		self::$client->writeTable($this->_tableId, $dataFile);
		unlink($dataFile);
	}

	/**
	 *
	 * Load data from Storage API
	 *
	 */
	public function load()
	{
		// If table not found, create a new one
		if (!self::$client->tableExists($this->_tableId)) {
			return;
		}

		$data = self::$client->exportTable($this->_tableId);

		$fh = tmpfile();
		fwrite($fh, $data);
		rewind($fh);
		$headers = fgetcsv($fh, null, ",", '"', '"');
		$data = fgetcsv($fh, null, ",", '"', '"');

		if (count($data) > 0 && count($headers) != count($data) || count($headers) == 0) {
			throw new OneLinerException("Cannot load data from {$this->_tableId}");
		}
		foreach($headers as $i => $header) {
			if (isset($data[$i])) {
				$this->$header = $data[$i];
			}
		}
	}

	/**
	 * @static
	 * @param Client $client
	 */
	public static function setClient(Client $client)
	{
		self::$client = $client;
	}

}