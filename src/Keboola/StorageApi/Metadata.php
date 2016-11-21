<?php

namespace Keboola\StorageApi;

use \Keboola\StorageApi\Options\Metadata\Metadatum;

class Metadata
{

	/**
	 * @var Client
	 */
	private $client;

	public function __construct(Client $client)
	{
		$this->client = $client;
	}

	/**
	 * @param $bucketId
	 * @return mixed|string list of metadata
	 */
	public function listBucketMetadata($bucketId)
	{
		return $this->client->apiGet("storage/buckets/{$bucketId}/metadata");
	}

	/**
	 * @param $tableId
	 * @return mixed|string list of metadata
	 */
	public function listTableMetadata($tableId)
	{
		return $this->client->apiGet("storage/tables/{$tableId}/metadata");
	}

	/**
	 * @param $columnId  -- ex: "in.c-bucket.table.column"
	 * @return mixed|string list of metadata
	 */
	public function listColumnMetadata($columnId)
	{
		return $this->client->apiGet("storage/columns/{$columnId}/metadata");
	}

	/**
	 * @param $bucketId
	 * @param Metadatum
	 * @return stringgit|mixed
	 */
	public function putBucketMetadata($bucketId, $metadatum)
	{
		return $this->client->apiPut("storage/buckets/{$bucketId}/metadata/{$metadatum['id']}", array(
			"key" => $metadatum['key'],
			"value" => $metadatum['value'],
			"provider" => $metadatum['provider']
		));
	}

	/**
	 * @param $tableId
	 * @param Metadatum
	 * @return stringgit|mixed
	 */
	public function putTableMetadata($tableId, Metadatum $metadatum)
	{
		return $this->client->apiPut("storage/tables/{$tableId}/metadata/{$metadatum['id']}", array(
			"key" => $metadatum['key'],
			"value" => $metadatum['value'],
			"provider" => $metadatum['provider']
		));
	}

	/**
	 * @param $columnId
	 * @param Metadatum
	 * @return stringgit|mixed
	 */
	public function putColumnMetadata($columnId, Metadatum $metadatum)
	{
		return $this->client->apiPut("storage/columns/{$columnId}/metadata/{$metadatum['id']}", array(
			"key" => $metadatum['key'],
			"value" => $metadatum['value'],
			"provider" => $metadatum['provider']
		));
	}

	/**
	 * @param $bucketId
	 * @param $metadataId
	 * @return mixed|string
	 */
	public function deleteBucketMetadata($bucketId, $metadataId)
	{
		return $this->client->apiDelete("storage/buckets/{$bucketId}/metadata/{$metadataId}");
	}

	/**
	 * @param $tableId
	 * @param $metadataId
	 * @return mixed|string
	 */
	public function deleteTableMetadata($tableId, $metadataId)
	{
		return $this->client->apiDelete("storage/tables/{$tableId}/metadata/{$metadataId}");
	}

	/**
	 * @param $columnId
	 * @param $metadataId
	 * @return mixed|string
	 */
	public function deleteColumnMetadata($columnId, $metadataId)
	{
		return $this->client->apiDelete("storage/columns/{$columnId}/metadata/{$metadataId}");
	}

	/**
	 * @param $bucketId
	 * @param array Metadatum $metadata
	 * @return mixed|string
	 * @throws ClientException
	 */
	public function postBucketMetadata($bucketId, $metadata)
	{
		if (!is_array($metadata) || count($metadata) === 0) {
			throw new ClientException("Second argument must be a non-empty array of metadata objects");
		}
		return $this->client->apiPost("storage/buckets/{$bucketId}/metadata", $metadata);
	}

	/**
	 * @param $tableId
	 * @param array Metadatum $metadata
	 * @return mixed|string
	 * @throws ClientException
	 */
	public function postTableMetadata($tableId, $metadata)
	{
		if (!is_array($metadata) || count($metadata) === 0) {
			throw new ClientException("Second argument must be a non-empty array of Metadata objects");
		}
		return $this->client->apiPost("storage/tables/{$tableId}/metadata", $metadata);
	}

	/**
	 * @param $columnId
	 * @param array Metadatum $metadata
	 * @return mixed|string
	 * @throws ClientException
	 */
	public function postColumnMetadata($columnId, $metadata)
	{
		if (!is_array($metadata) || count($metadata) === 0) {
			throw new ClientException("Second argument must be a non-empty array of Metadata objects");
		}
		return $this->client->apiPost("storage/columns/{$columnId}/metadata", $metadata);
	}
}