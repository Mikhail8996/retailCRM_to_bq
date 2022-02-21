SELECT productName, sum((initialPrice	 * quantity) - discountTotal) as sumSale, sum(quantity) as quantity
FROM `api-parser-341913.test_task.products` 
group by productName
order by quantity desc