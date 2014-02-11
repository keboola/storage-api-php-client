<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 06/02/14
 * Time: 10:43
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Options;

class ListFilesOptions
{
	private $tags = array();

	private $limit = 100;

	private $offset = 0;

	private $query;

	/**
	 * @return array
	 */
	public function getTags()
	{
		return $this->tags;
	}

	/**
	 * @param array $tags
	 * @return $this
	 */
	public function setTags(array $tags)
	{
		$this->tags = $tags;
		return $this;
	}

	/**
	 * @return int
	 */
	public function getLimit()
	{
		return $this->limit;
	}

	/**
	 * @param $limit
	 * @return $this
	 */
	public function setLimit($limit)
	{
		$this->limit = (int) $limit;
		return $this;
	}

	/**
	 * @return int
	 */
	public function getOffset()
	{
		return $this->offset;
	}

	/**
	 * @param $offset
	 * @return $this
	 */
	public function setOffset($offset)
	{
		$this->offset = (int) $offset;
		return $this;
	}

	public function toArray()
	{
		return array(
			'limit' => $this->getLimit(),
			'offset' => $this->getOffset(),
			'tags' => $this->getTags(),
			'q' => $this->getQuery(),
		);
	}

	/**
	 * @return mixed
	 */
	public function getQuery()
	{
		return $this->query;
	}

	/**
	 * @param $query
	 * @return $this
	 */
	public function setQuery($query)
	{
		$this->query = $query;
		return $this;
	}

}