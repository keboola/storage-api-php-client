<?php
/**
 * Storage API Client - Table abstraction
 *
 * Useful for data insertion to Storage API from array or string, without need to write temporary CSV files.
 * Temporary CSV file creation is handled by this class.
 *
 * @author Miroslav Cillik <miro@keboola.com>
 * @date: 25.9.12
 */

namespace Keboola\StorageApi;

use Keboola\Csv\CsvFile;

class Table
{
	/**
	 * @var string
	 */
	protected $id;

	/**
	 * @var string
	 */
	protected $_name;

	/**
	 * @var string
	 */
	protected $_bucketId;

	/**
	 * @var string
	 */
	protected $_filename;

	/**
	 * @var Client
	 */
	protected $_client;

	/**
	 * Header columns array
	 *
	 * @var array
	 */
	protected $_header;

	/**
	 * 2 dimensional array of data - Rows x Columns
	 *
	 * @var array
	 */
	protected $_data = array();

	/**
	 * key value pairs of attributes
	 *
	 * @var array
	 */
	protected $_attributes = array();

	/**
	 * array of indices to add
	 *
	 * @var array
	 */
	protected $_indices = array();

	/**
	 * @var bool
	 */
	protected $_incremental = false;

	/**
	 * @var bool
	 */
	protected $_transactional = false;

	protected $_delimiter = ',';

	protected $_enclosure = '"';

	protected $_partial = false;

	protected $_primaryKey;

	/**
	 * @param Client $client
	 * @param string $id - table ID
	 * @param string $filename - path to csv file (optional)
	 * @param null $primaryKey
	 * @param bool $transactional
	 * @param string $delimiter
	 * @param string $enclosure
	 * @param bool $incremental
	 * @param bool $partial
	 */
	public function __construct(Client $client, $id, $filename = '', $primaryKey=null,
        $transactional=false, $delimiter=',', $enclosure='"', $incremental=false, $partial=false
	) {
		$this->_client = $client;
		$this->_id = $id;
		$this->_filename = $filename;

		$tableNameArr = explode('.', $id);
		$this->_name = $tableNameArr[2];

		$bucketName = $tableNameArr[1];
		$stage = $tableNameArr[0];

		$this->_bucketId = $this->_client->getBucketId($bucketName, $stage);

		$this->_transactional = $transactional;
		$this->_delimiter = $delimiter;
		$this->_enclosure = $enclosure;
		$this->_incremental = $incremental;
		$this->_partial = $partial;
		$this->_primaryKey = $primaryKey;
	}

	/**
	 * @return string
	 */
	public function getId()
	{
		return $this->_id;
	}

	/**
	 * @return string
	 */
	public function getName()
	{
		return $this->_name;
	}

	/**
	 * @return string
	 */
	public function getFilename()
	{
		return $this->_filename;
	}

	/**
	 * @param bool $bool
	 */
	public function setTransactional($bool)
	{
		$this->_transactional = $bool;
	}

	/**
	 * @param bool $bool
	 */
	public function setIncremental($bool)
	{
		$this->_incremental = $bool;
	}

	/**
	 * @param bool $bool
	 */
	public function setPartial($bool)
	{
		$this->_partial = $bool;
	}

	/**
	 * @return bool
	 */
	public function isTransactional()
	{
		return $this->_transactional;
	}

	/**
	 * @return bool
	 */
	public function isIncremental()
	{
		return $this->_incremental;
	}

	/**
	 * @return bool
	 */
	public function isPartial()
	{
		return $this->_partial;
	}

	/**
	 * @return string
	 */
	public function getBucketId()
	{
		return $this->_bucketId;
	}

	/**
	 * @return array
	 */
	public function getHeader()
	{
		return $this->_header;
	}

	/**
	 * @return array
	 */
	public function getData()
	{
		return $this->_data;
	}

	/**
	 * @param string $filename
	 */
	public function setFilename($filename)
	{
		$this->_filename = $filename;
	}

	/**
	 * @param array $header
	 */
	public function setHeader($header)
	{
		$this->_header = self::normalizeHeader($header);
	}

	/**
	 * @param $key
	 * @param $value
	 */
	public function setAttribute($key, $value)
	{
		$this->_attributes[$key] = $value;
	}

	/**
	 * @return string
	 */
	public function getDelimiter()
	{
		return $this->_delimiter;
	}

	/**
	 * @param string $delim
	 */
	public function setDelimiter($delim)
	{
		$this->_delimiter = $delim;
	}

	/**
	 * @param string $enc
	 */
	public function setEnclosure($enc)
	{
		$this->_enclosure = $enc;
	}

	/**
	 * @return string
	 */
	public function getEnclosure()
	{
		return $this->_enclosure;
	}

	/**
	 * @param $key
	 * @return string
	 */
	public function getAttribute($key)
	{
		return $this->_attributes[$key];
	}

	/**
	 * @return array
	 */
	public function getAttributes()
	{
		return $this->_attributes;
	}

	public function addIndex($index)
	{
		$this->_indices[] = $index;
	}

	public function setIndices(array $indices)
	{
		$this->_indices = $indices;
	}

	public function getIndices()
	{
		return $this->_indices;
	}

	/**
	 * @param array $data
	 * @param bool $hasHeader
	 * @throws TableException
	 */
	public function setFromArray($data, $hasHeader=false)
	{
		if (!is_array($this->_data)) {
			throw new TableException('Invalid data type - expected 2D Array');
		}

		if ($hasHeader) {
			$this->setHeader(array_shift($data));
		}

		$this->_data = $data;
	}

	public function setFromString($string, $delimiter=',', $enclosure='"', $hasHeader=false)
	{
		$data = self::csvStringToArray($string, $delimiter, $enclosure);
		$this->setFromArray($data, $hasHeader);
	}

	/**
	 * Save data and table attributes to Storage API
	 */
	public function save($async = false)
	{
		if (!empty($this->_filename)) {
			$tempfile = $this->_filename;
		} else {
			$this->_preSave();

			$tempfile = tempnam(__DIR__ . "/tmp/", 'sapi-client-' . $this->_id . '-');
			$file = new \Keboola\Csv\CsvFile($tempfile);
			$file->writeRow($this->_header);
			foreach ($this->_data as $row) {
				$file->writeRow($row);
			}
			// Close the file
			unset($file);
		}

		try {
			$method = 'createTable';
			if ($async) {
				$method .= 'Async';
			}
			$this->_client->$method(
				$this->_bucketId,
				$this->_name,
				new CsvFile($tempfile, $this->_delimiter, $this->_enclosure),
				array(
					'primaryKey' => $this->_primaryKey,
					'transactional' =>
					$this->_transactional
				)
			);
		} catch (ClientException $e) {
			$method = 'writeTable';
			if ($async) {
				$method .= 'Async';
			}
			$this->_client->$method(
				$this->_id,
				new CsvFile($tempfile,$this->_delimiter, $this->_enclosure),
				array(
					'transactional' => $this->_transactional,
					'incremental' => $this->_incremental,
					'partial' => $this->_partial
				)
			);
		}

		// Save table attributes
		foreach ($this->_attributes as $k => $v) {
			$this->_client->setTableAttribute($this->_id, $k, $v);
		}

		// Add table indices
		foreach ($this->_indices as $v) {
			$this->_client->markTableColumnAsIndexed($this->_id, $v);
		}

		unlink($tempfile);
	}

	protected function _preSave()
	{
		if (empty($this->_header)) {
			throw new TableException('Empty header. Header must be set.');
		}
	}

	public static function normalizeHeader(&$header)
	{
		$emptyCnt = 0;
		foreach($header as &$col) {
			$col = self::removeSpecialChars($col);
			if ($col == 'col') {
				$col .= $emptyCnt;
				$emptyCnt++;
			}
		}

		return $header;
	}

	public static function removeSpecialChars($string)
	{
		$string = str_replace('#', 'count', $string);
		$string = preg_replace("/[^A-Za-z0-9_\s]/", '', $string);
		$string = trim($string);
		$string = str_replace(' ', '_', $string);
		$string = lcfirst($string);

		if (!strlen($string)) {
			$string = 'col';
		}

		return $string;
	}

	public static function csvStringToArray($string, $delimiter = ',', $enclosure = '"')
	{
		$result = array();
		$rows = explode("\n", $string);

		foreach($rows as $row) {
			if (!empty($row)) {
				$result[] = str_getcsv($row, $delimiter, $enclosure, $enclosure);
			}
		}

		return $result;
	}
}
