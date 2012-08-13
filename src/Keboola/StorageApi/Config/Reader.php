<?
namespace Keboola\StorageApi\Config;
use \Keboola\StorageApi;
class Reader
{

	/**
	 *
	 * Key nest separator
	 *
	 * @var string
	 */
	public static $nestSeparator = ".";

	/**
	 *
	 * Array identifier
	 *
	 * @var string
	 */
	public static $container = "items";

	/**
	 * @var \Keboola\StorageApi\Client
	 */
	public static $client;

	/**
	 *
	 * load and return data
	 *
	 * @static
	 * @param $token
	 * @param $bucket
	 * @return array|string
	 */
	public static function read($bucket)
	{
		$sApiArray = self::load($bucket);
		return self::parse($sApiArray);

	}

	/**
	 *
	 * Parse key/value
	 *
	 * @static
	 * @param $data
	 * @return array|string
	 */
	protected static function parse($data)
	{
		if (!is_array($data)) {
			return trim($data);
		}

		$return = array();

		foreach($data as $key => $value) {
			if (strpos($key, self::$nestSeparator) !== false) {
				$pieces = explode(self::$nestSeparator, $key, 3);
				if (count($pieces) == 2) {
					$return[$pieces[0]][$pieces[1]] = self::parse($value);
				}
				if (count($pieces) == 3) {
					$return[$pieces[0]][$pieces[1]][$pieces[2]] = self::parse($value);
				}
			} else {
				$return[$key] = self::parse($value);
			}
		}
		return $return;
	}

	/**
	 *
	 * Load from StorageApi
	 *
	 * @param $token
	 * @param $bucket
	 * @return array
	 */
	protected static function load($bucket)
	{
		$data = array();
		$sApi = self::$client;

		if (!$sApi->bucketExists($bucket)) {
			throw new Exception("Configuration bucket '{$bucket}' not found");
		}

		// Bucket attributes
		$bucketInfo = $sApi->getBucket($bucket);
		if ($bucketInfo["attributes"]) {
			$data = array_merge($data, $bucketInfo["attributes"]);
		}

		// Tables
		foreach($sApi->listTables($bucket) as $table) {
			$tableInfo = $sApi->getTable($table["id"]);

			if ($tableInfo["attributes"]) {
				$data[self::$container][$table["name"]] = $tableInfo["attributes"];
			}

			$csvData = $sApi->exportTable($table["id"]);
			if ($csvData) {
				$data[self::$container][$table["name"]][self::$container] = \Keboola\StorageApi\Client::parseCsv($csvData);
			}
		}
		return $data;
	}
}