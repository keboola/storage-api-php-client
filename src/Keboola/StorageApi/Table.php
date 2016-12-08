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

/**
 * Class Table
 * @package Keboola\StorageApi
 * @deprecated
 */
class Table
{
    /**
     * @var string
     */
    protected $id;

    /**
     * @var string
     */
    protected $name;

    /**
     * @var string
     */
    protected $bucketId;

    /**
     * @var string
     */
    protected $filename;

    /**
     * @var Client
     */
    protected $client;

    /**
     * Header columns array
     *
     * @var array
     */
    protected $header;

    /**
     * 2 dimensional array of data - Rows x Columns
     *
     * @var array
     */
    protected $data = array();

    /**
     * key value pairs of attributes
     *
     * @var array
     */
    protected $attributes = array();

    /**
     * array of indices to add
     *
     * @var array
     */
    protected $indices = array();

    /**
     * @var bool
     */
    protected $incremental = false;

    /**
     * @var bool
     */
    protected $transactional = false;

    protected $delimiter = ',';

    protected $enclosure = '"';

    protected $partial = false;

    protected $primaryKey;

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
     * @throws TableException
     */
    public function __construct(
        Client $client,
        $id,
        $filename = '',
        $primaryKey = null,
        $transactional = false,
        $delimiter = ',',
        $enclosure = '"',
        $incremental = false,
        $partial = false
    ) {
    
        $this->client = $client;
        $this->_id = $id;
        $this->filename = $filename;

        $tableNameArr = explode('.', $id);
        if (count($tableNameArr) != 3) {
            throw new TableException('Invalid table name - string in form "stage.bucket.table" expected.');
        }
        $this->name = $tableNameArr[2];

        $bucketName = $tableNameArr[1];
        $stage = $tableNameArr[0];

        $this->bucketId = $this->client->getBucketId($bucketName, $stage);
        if (!$this->bucketId) {
            throw new TableException("Bucket {$bucketName} not found in stage {$stage}.");
        }

        $this->transactional = $transactional;
        $this->delimiter = $delimiter;
        $this->enclosure = $enclosure;
        $this->incremental = $incremental;
        $this->partial = $partial;
        $this->primaryKey = $primaryKey;
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
        return $this->name;
    }

    /**
     * @return string
     */
    public function getFilename()
    {
        return $this->filename;
    }

    /**
     * @param bool $bool
     */
    public function setTransactional($bool)
    {
        $this->transactional = $bool;
    }

    /**
     * @param bool $bool
     */
    public function setIncremental($bool)
    {
        $this->incremental = $bool;
    }

    /**
     * @param bool $bool
     */
    public function setPartial($bool)
    {
        $this->partial = $bool;
    }

    /**
     * @return bool
     */
    public function isTransactional()
    {
        return $this->transactional;
    }

    /**
     * @return bool
     */
    public function isIncremental()
    {
        return $this->incremental;
    }

    /**
     * @return bool
     */
    public function isPartial()
    {
        return $this->partial;
    }

    /**
     * @return string
     */
    public function getBucketId()
    {
        return $this->bucketId;
    }

    /**
     * @return array
     */
    public function getHeader()
    {
        return $this->header;
    }

    /**
     * @return array
     */
    public function getData()
    {
        return $this->data;
    }

    /**
     * @param string $filename
     */
    public function setFilename($filename)
    {
        $this->filename = $filename;
    }

    /**
     * @param array $header
     */
    public function setHeader($header)
    {
        $this->header = self::normalizeHeader($header);
    }

    /**
     * @param $key
     * @param $value
     */
    public function setAttribute($key, $value)
    {
        $this->attributes[$key] = $value;
    }

    /**
     * @return string
     */
    public function getDelimiter()
    {
        return $this->delimiter;
    }

    /**
     * @param string $delim
     */
    public function setDelimiter($delim)
    {
        $this->delimiter = $delim;
    }

    /**
     * @param string $enc
     */
    public function setEnclosure($enc)
    {
        $this->enclosure = $enc;
    }

    /**
     * @return string
     */
    public function getEnclosure()
    {
        return $this->enclosure;
    }

    /**
     * @param $key
     * @return string
     */
    public function getAttribute($key)
    {
        return $this->attributes[$key];
    }

    /**
     * @return array
     */
    public function getAttributes()
    {
        return $this->attributes;
    }

    public function addIndex($index)
    {
        $this->indices[] = $index;
    }

    public function setIndices(array $indices)
    {
        $this->indices = $indices;
    }

    public function getIndices()
    {
        return $this->indices;
    }

    /**
     * @param array $data
     * @param bool $hasHeader
     * @throws TableException
     */
    public function setFromArray($data, $hasHeader = false)
    {
        if (!is_array($this->data)) {
            throw new TableException('Invalid data type - expected 2D Array');
        }

        if ($hasHeader) {
            $this->setHeader(array_shift($data));
        }

        $this->data = $data;
    }

    public function setFromString($string, $delimiter = ',', $enclosure = '"', $hasHeader = false)
    {
        $data = self::csvStringToArray($string, $delimiter, $enclosure);
        $this->setFromArray($data, $hasHeader);
    }

    /**
     * Save data and table attributes to Storage API
     */
    public function save($async = false)
    {
        if (!empty($this->filename)) {
            $tempfile = $this->filename;
        } else {
            $this->preSave();

            $tempfile = tempnam(sys_get_temp_dir(), 'sapi-client-' . $this->_id . '-');
            $file = new CsvFile($tempfile, $this->delimiter, $this->enclosure);
            $file->writeRow($this->header);
            foreach ($this->data as $row) {
                $file->writeRow($row);
            }
            // Close the file
            unset($file);
        }

        if (!$this->client->tableExists($this->_id)) {
            $method = 'createTable';
            if ($async) {
                $method .= 'Async';
            }
            $this->client->$method(
                $this->bucketId,
                $this->name,
                new CsvFile($tempfile, $this->delimiter, $this->enclosure),
                array(
                    'primaryKey' => $this->primaryKey,
                    'transactional' =>
                        $this->transactional
                )
            );
        } else {
            $method = 'writeTable';
            if ($async) {
                $method .= 'Async';
            }
            $this->client->$method(
                $this->_id,
                new CsvFile($tempfile, $this->delimiter, $this->enclosure),
                array(
                    'transactional' => $this->transactional,
                    'incremental' => $this->incremental,
                    'partial' => $this->partial
                )
            );
        }

        // Save table attributes
        foreach ($this->attributes as $k => $v) {
            $this->client->setTableAttribute($this->_id, $k, $v);
        }

        // Add table indices
        foreach ($this->indices as $v) {
            $this->client->markTableColumnAsIndexed($this->_id, $v);
        }

        unlink($tempfile);
    }

    protected function preSave()
    {
        if (empty($this->header)) {
            throw new TableException('Empty header. Header must be set.');
        }
    }

    public static function normalizeHeader(&$header)
    {
        $emptyCnt = 0;
        foreach ($header as &$col) {
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

        if (!strlen($string)) {
            $string = 'col';
        }

        return $string;
    }

    public static function csvStringToArray($string, $delimiter = ',', $enclosure = '"')
    {
        $result = array();
        $rows = explode("\n", $string);

        foreach ($rows as $row) {
            if (!empty($row)) {
                $result[] = str_getcsv($row, $delimiter, $enclosure, $enclosure);
            }
        }

        return $result;
    }
}
