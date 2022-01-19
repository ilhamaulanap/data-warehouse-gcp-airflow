ALTER TABLE
  `test-project-335210.recipe_staging_dataset.dataset_ayam` ADD COLUMN
IF NOT EXISTS Main_Ingredient STRING;
CREATE OR REPLACE TABLE
  `test-project-335210.recipe_dataset.D_dataset_ayam` AS
SELECT
  IFNULL(Main_Ingredient,
    'Ayam') AS Main_Ingredient,
  Title AS Recipe_Title,
  Ingredients,
  Steps,
  Loves,
  URL
FROM
  `test-project-335210.recipe_staging_dataset.dataset_ayam`;