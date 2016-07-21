<?php
namespace Keboola\StorageApi\Config;

use \Keboola\StorageApi;

/**
 * Class Reader
 * @package Keboola\StorageApi\Config
 * @deprecated
 */
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
     * @param string $bucket
     * @param string null $token
     * @param bool $readCsvData if the config reader should read CSV data from all tables (is much slower)
     * @return array|string
     */
    public static function read($bucket, $token = null, $readCsvData = true)
    {
        if ($token) {
            self::$client = new \Keboola\StorageApi\Client(array(
                'token' => $token,
            ));
        }

        $sApiArray = self::load($bucket, $readCsvData);
        return self::parse($sApiArray);
    }

    /**
     *
     * Parse key/value
     *
     * @static
     * @param array|string $data
     * @return array|string
     */
    protected static function parse($data)
    {
        if (!is_array($data)) {
            return trim($data);
        }

        $return = array();

        foreach ($data as $key => $value) {
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
     * @param string $bucket
     * @param bool $readCsvData
     * @throws Exception
     * @return array
     */
    protected static function load($bucket, $readCsvData = true)
    {
        $data = array();
        $sApi = self::$client;

        if (!$sApi->bucketExists($bucket)) {
            throw new Exception("Configuration bucket '{$bucket}' not found");
        }

        // Bucket attributes
        $bucketInfo = $sApi->getBucket($bucket);
        if ($bucketInfo["attributes"]) {
            $data = array_merge($data, self::attributesKeyValueMap($bucketInfo["attributes"]));
        }

        // Tables
        foreach ($bucketInfo["tables"] as $table) {
            if ($table["attributes"]) {
                $data[self::$container][$table["name"]] = self::attributesKeyValueMap($table["attributes"]);
            }
            if ($readCsvData) {
                $csvData = $sApi->exportTable($table["id"]);
                if ($csvData) {
                    $data[self::$container][$table["name"]][self::$container] = \Keboola\StorageApi\Client::parseCsv($csvData);
                }
            }
        }
        return $data;
    }

    private static function attributesKeyValueMap($attributes)
    {
        $map = array();
        foreach ($attributes as $attribute) {
            $map[$attribute['name']] = $attribute['value'];
        }
        return $map;
    }
}
