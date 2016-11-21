<?php

namespace Keboola\StorageApi\Options\Metadata;


class Metadatum {

	private $metadataId;

	private $key;

	private $value;

	private $provider;

	/**
	 * @return mixed
	 */
	public function getKey()
	{
		return $this->key;
	}

	/**
	 * @param mixed $key
	 */
	public function setKey($key)
	{
		$this->key = $key;
		return $this;
	}

	/**
	 * @return mixed
	 */
	public function getValue()
	{
		return $this->value;
	}

	/**
	 * @param mixed $value
	 */
	public function setValue($value)
	{
		$this->value = $value;
		return $this;
	}

	/**
	 * @return mixed
	 */
	public function getProvider()
	{
		return $this->provider;
	}

	/**
	 * @param mixed $provider
	 */
	public function setProvider($provider)
	{
		$this->provider = $provider;
		return $this;
	}

	/**
	 * @param mixed $id
	 */
	public function setMetadataId($id) {
		$this->metadataId = $id;
		return $this;
	}

	/**
 	* @return mixed
 	*/
	public function getMetadataId()
	{
		return $this->metadataId;
	}

	public function setFromArray($metadata)
	{
		if (isset($metadata['id'])) {
			$this->setMetadataId($metadata['id']);
		}
		return $this->setKey($metadata['key'])
			->setValue($metadata['value'])
			->setProvider($metadata['provider']);
	}
}