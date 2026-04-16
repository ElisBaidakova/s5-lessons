-- Удалите внешний ключ из sales
alter table public.sales DROP CONSTRAINT sales_products_product_id_fk;

-- Удалите первичный ключ из products
ALTER TABLE public.products drop CONSTRAINT products_pk;

-- Добавьте новое поле id для суррогантного ключа в products
alter table public.products rename column product_id to id;

-- Сделайте данное поле первичным ключом
CREATE SEQUENCE public.products_id_seq;
ALTER TABLE public.products ALTER COLUMN id SET DEFAULT nextval('public.products_id_seq'::regclass);
ALTER SEQUENCE public.products_id_seq OWNED BY public.products.id;
ALTER TABLE public.products ADD CONSTRAINT products_pk PRIMARY KEY(id);

-- Добавьте дату начала действия записи в products
alter table public.products add column valid_from timestamptz NOT NULL;

-- Добавьте дату окончания действия записи в products
alter table public.products add column valid_to timestamptz;

-- Добавьте новый внешний ключ sales_products_id_fk в sales
ALTER TABLE public.sales ADD CONSTRAINT sales_products_id_fk FOREIGN KEY (product_id)
REFERENCES public.products(id);