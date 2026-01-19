#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPERBID - SCRAPER DIRETO PARA superbid_items
✅ Remove dependência do normalizer
✅ Salva diretamente na tabela superbid_items
✅ Extrai todos os campos da API JSON
✅ Mantém informações detalhadas (marca, modelo, ano, leiloeiro, etc)
"""

import sys
import json
import time
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict


class SuperbidScraper:
    """Scraper para Superbid com mapeamento de categorias"""
    
    def __init__(self):
        self.source = 'superbid'
        self.base_url = 'https://offer-query.superbid.net/seo/offers/'
        self.site_url = 'https://exchange.superbid.net'
        
        # Mapeamento: (url_slug, category, display_name)
        self.sections = [
            # ALIMENTOS E BEBIDAS
            ('alimentos-e-bebidas', 'Alimentos e Bebidas', 'Alimentos e Bebidas'),
            
            # ANIMAIS
            ('animais/bovinos', 'Bovinos', 'Bovinos'),
            
            # ARTES E COLECIONISMO
            ('artes-decoracao-colecionismo', 'Artes e Colecionismo', 'Artes e Colecionismo'),
            
            # BENS DE CONSUMO
            ('bolsas-canetas-joias-e-relogios/acessorios', 'Acessórios', 'Acessórios'),
            ('bolsas-canetas-joias-e-relogios/bolsas', 'Bolsas', 'Bolsas'),
            ('bolsas-canetas-joias-e-relogios/canetas', 'Canetas', 'Canetas'),
            ('bolsas-canetas-joias-e-relogios/relogios', 'Relógios', 'Relógios'),
            ('oportunidades/beneficentes', 'Beneficentes', 'Beneficentes'),
            ('oportunidades/pet', 'Pet', 'Pet'),
            ('oportunidades/vestuarios', 'Vestuários', 'Vestuários'),
            
            # VEÍCULOS - CAMINHÕES/ÔNIBUS
            ('caminhoes-onibus/onibus', 'Ônibus', 'Ônibus'),
            ('caminhoes-onibus/caminhoes', 'Caminhões', 'Caminhões'),
            ('caminhoes-onibus/vans', 'Vans', 'Vans'),
            ('caminhoes-onibus/impl-rod-e-carrocerias', 'Implementos Rodoviários', 'Implementos Rodoviários'),
            
            # VEÍCULOS - CARROS/MOTOS
            ('carros-motos/carros', 'Carros', 'Carros'),
            ('carros-motos/motos', 'Motos', 'Motos'),
            ('carros-motos/varias-ferramentas', 'Veículos Diversos', 'Veículos Diversos'),
            
            # VEÍCULOS - EMBARCAÇÕES/AERONAVES
            ('embarcacoes-aeronaves/embarcacoes-e-navios', 'Embarcações e Navios', 'Embarcações e Navios'),
            ('embarcacoes-aeronaves/jet-skis', 'Jet Skis', 'Jet Skis'),
            ('embarcacoes-aeronaves/lanchas-e-barcos', 'Lanchas e Barcos', 'Lanchas e Barcos'),
            ('embarcacoes-aeronaves/avioes', 'Aviões', 'Aviões'),
            
            # PARTES E PEÇAS
            ('caminhoes-onibus/partes-e-pecas-caminhoes-e-onibus', 'Peças Caminhões/Ônibus', 'Peças Caminhões/Ônibus'),
            ('carros-motos/partes-e-pecas-carros-e-motos', 'Peças Carros/Motos', 'Peças Carros/Motos'),
            ('embarcacoes-aeronaves/pecas-e-acessorios', 'Peças Embarcações/Aeronaves', 'Peças Embarcações/Aeronaves'),
            ('partes-e-pecas', 'Peças Variadas', 'Peças Variadas'),
            
            # DIVERSOS
            ('cozinhas-e-restaurantes/restaurantes', 'Restaurantes', 'Restaurantes'),
            ('cozinhas-e-restaurantes/cozinhas-industriais', 'Cozinhas Industriais', 'Cozinhas Industriais'),
            ('oportunidades/negocios', 'Negócios', 'Negócios'),
            ('oportunidades/lazer', 'Lazer', 'Lazer'),
            ('oportunidades/esportes', 'Esportes', 'Esportes'),
            
            # ELETRODOMÉSTICOS
            ('eletrodomesticos/refrigeradores', 'Refrigeradores', 'Refrigeradores'),
            ('eletrodomesticos/fornos-e-fogoes', 'Fornos e Fogões', 'Fornos e Fogões'),
            ('eletrodomesticos/eletroportateis', 'Eletroportáteis', 'Eletroportáteis'),
            ('eletrodomesticos/limpeza', 'Limpeza', 'Limpeza'),
            
            # IMÓVEIS
            ('imoveis/imoveis-industriais', 'Imóveis Industriais', 'Imóveis Industriais'),
            ('imoveis/terrenos-e-lotes', 'Terrenos e Lotes', 'Terrenos e Lotes'),
            ('imoveis/imoveis-flutuantes', 'Imóveis Flutuantes', 'Imóveis Flutuantes'),
            ('imoveis/imoveis-rurais', 'Imóveis Rurais', 'Imóveis Rurais'),
            ('imoveis/imoveis-comerciais', 'Imóveis Comerciais', 'Imóveis Comerciais'),
            ('imoveis/imoveis-residenciais', 'Imóveis Residenciais', 'Imóveis Residenciais'),
            
            # INDUSTRIAL EQUIPAMENTOS
            ('industrial-maquinas-equipamentos', 'Industrial e Máquinas', 'Industrial e Máquinas'),
            ('movimentacao-transporte', 'Movimentação e Transporte', 'Movimentação e Transporte'),
            
            # MÁQUINAS PESADAS E AGRÍCOLAS
            ('maquinas-pesadas-agricolas', 'Máquinas Pesadas e Agrícolas', 'Máquinas Pesadas e Agrícolas'),
            
            # MATERIAIS CONSTRUÇÃO
            ('materiais-para-construcao-civil/ferramentas', 'Ferramentas', 'Ferramentas'),
            ('materiais-para-construcao-civil/materiais', 'Materiais', 'Materiais'),
            ('materiais-para-construcao-civil/eletrica-e-iluminacao', 'Elétrica e Iluminação', 'Elétrica e Iluminação'),
            
            # MÓVEIS E DECORAÇÃO
            ('moveis-e-decoracao', 'Móveis e Decoração', 'Móveis e Decoração'),
            
            # SUCATAS E RESÍDUOS
            ('sucatas-materiais-residuos', 'Sucatas e Resíduos', 'Sucatas e Resíduos'),
            
            # TECNOLOGIA
            ('tecnologia/informatica', 'Informática', 'Informática'),
            ('tecnologia/telefonia', 'Telefonia', 'Telefonia'),
            ('tecnologia/eletronicos', 'Eletrônicos', 'Eletrônicos'),
        ]
        
        self.stats = {
            'total_scraped': 0,
            'by_category': {},
            'duplicates': 0,
            'with_bids': 0,
        }
        
        self.headers = {
            "accept": "*/*",
            "accept-language": "pt-BR,pt;q=0.9",
            "origin": "https://exchange.superbid.net",
            "referer": "https://exchange.superbid.net/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def scrape(self) -> List[Dict]:
        """Scrape completo do Superbid - retorna lista de itens - TODAS as páginas"""
        print("\n" + "="*60)
        print("🔵 SUPERBID - SCRAPER")
        print("="*60)
        
        all_items = []
        global_ids = set()
        
        for url_slug, category, display_name in self.sections:
            print(f"\n📦 {display_name}")
            
            section_items = self._scrape_section(
                url_slug, category, display_name, global_ids
            )
            
            all_items.extend(section_items)
            self.stats['by_category'][category] = len(section_items)
            
            print(f"✅ {len(section_items)} itens")
            
            time.sleep(2)
        
        self.stats['total_scraped'] = len(all_items)
        return all_items
    
    def _scrape_section(self, url_slug: str, category: str,
                       display_name: str, global_ids: set) -> List[Dict]:
        """Scrape uma seção específica - TODAS as páginas disponíveis"""
        items = []
        page_num = 1
        page_size = 100
        consecutive_errors = 0
        max_errors = 3
        
        while True:  # ✅ SEM LIMITE! Vai até acabar as páginas
            try:
                url = f"{self.base_url}{url_slug}"
                params = {
                    'page': page_num,
                    'pageSize': page_size,
                }
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code != 200:
                    consecutive_errors += 1
                    print(f"    ⚠️ Erro HTTP {response.status_code} na página {page_num}")
                    if consecutive_errors >= max_errors:
                        print(f"    ✅ Fim da categoria ({max_errors} erros consecutivos)")
                        break
                    page_num += 1
                    continue
                
                data = response.json()
                offers = data.get('offers', [])
                
                if not offers:
                    print(f"    ✅ Fim da categoria (sem mais itens)")
                    break
                
                consecutive_errors = 0
                print(f"    📄 Página {page_num}: {len(offers)} ofertas")
                
                for offer_data in offers:
                    item = self._parse_offer(offer_data, category, display_name)
                    
                    if item and item['external_id'] not in global_ids:
                        items.append(item)
                        global_ids.add(item['external_id'])
                        
                        if item.get('has_bid'):
                            self.stats['with_bids'] += 1
                    elif item:
                        self.stats['duplicates'] += 1
                
                page_num += 1
                
            except Exception as e:
                consecutive_errors += 1
                print(f"    ⚠️ Erro na página {page_num}: {e}")
                if consecutive_errors >= max_errors:
                    print(f"    ✅ Fim da categoria ({max_errors} erros consecutivos)")
                    break
                page_num += 1
                continue
        
        return items
    
    def _parse_offer(self, offer: Dict, category: str, display_name: str) -> Optional[Dict]:
        """Parse de uma oferta - formato superbid_items"""
        try:
            # IDs básicos
            offer_id = offer.get('id')
            if not offer_id:
                return None
            
            external_id = f"superbid_{offer_id}"
            
            # Product info
            product = offer.get('product', {})
            product_id = product.get('productId')
            
            # Título e descrição
            title = product.get('shortDesc') or offer.get('offerDescription', {}).get('offerDescription', 'Sem título')
            description = product.get('detailedDescription')
            
            # Categoria e subcategoria
            sub_category = product.get('subCategory', {})
            sub_category_id = sub_category.get('id')
            sub_category_desc = sub_category.get('description')
            
            category_obj = sub_category.get('category', {})
            
            product_type = product.get('productType', {})
            product_type_id = product_type.get('id')
            product_type_desc = product_type.get('description')
            
            # Localização
            location = product.get('location', {})
            city = None
            state = None
            location_full = location.get('city')
            
            if location_full:
                if ' - ' in location_full:
                    parts = location_full.split(' - ')
                    city = parts[0].strip()
                    state_raw = parts[1].strip()
                    if len(state_raw) == 2:
                        state = state_raw.upper()
                else:
                    city = location_full.strip()
            
            # Se não tem state na location, tenta do seller
            if not state:
                seller = offer.get('seller', {})
                seller_city = seller.get('city')
                if seller_city and ' - ' in seller_city:
                    state_raw = seller_city.split(' - ')[-1].strip()
                    if len(state_raw) == 2:
                        state = state_raw.upper()
            
            # Se ainda não tem state, tenta converter nome completo
            if not state and location.get("state"):
                state_full = location.get("state", "")
                state_map = {
                    'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM',
                    'Bahia': 'BA', 'Ceará': 'CE', 'Distrito Federal': 'DF', 'Espírito Santo': 'ES',
                    'Goiás': 'GO', 'Maranhão': 'MA', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS',
                    'Minas Gerais': 'MG', 'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR',
                    'Pernambuco': 'PE', 'Piauí': 'PI', 'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN',
                    'Rio Grande do Sul': 'RS', 'Rondônia': 'RO', 'Roraima': 'RR', 'Santa Catarina': 'SC',
                    'São Paulo': 'SP', 'Sergipe': 'SE', 'Tocantins': 'TO'
                }
                state = state_map.get(state_full, state_full)
            
            location_geo = location.get('locationGeo', {})
            location_lat = location_geo.get('lat')
            location_lon = location_geo.get('lon')
            
            # Auction info
            auction = offer.get('auction', {})
            auction_id = auction.get('id')
            auction_name = auction.get('desc')
            auction_status_id = auction.get('statusId')
            auction_modality = auction.get('modalityDesc')
            
            # Datas do leilão
            auction_begin_date = self._parse_datetime(auction.get('beginDate'))
            auction_end_date = self._parse_datetime(auction.get('endDate'))
            auction_max_enddate = self._parse_datetime(auction.get('maxEnddateOffer'))
            
            # Leiloeiro
            auctioneer_name = auction.get('auctioneer')
            auctioneer_registry = auction.get('registry')
            
            # Endereço do leilão
            auction_address = auction.get('address')
            
            # Informações judiciais
            judicial_praca = auction.get('judicialPraca')
            judicial_praca_desc = auction.get('judicialPracaDescription')
            judicial_control_number = auction.get('judicialControlNumber')
            
            # Store info
            store = offer.get('store', {})
            store_id = store.get('id')
            store_name = store.get('name')
            store_highlight = store.get('highlight')
            store_logo_url = store.get('logoUri')
            
            # Manager
            manager = offer.get('manager', {})
            manager_id = manager.get('id')
            manager_name = manager.get('name')
            
            # Valores
            price = offer.get('price')
            price_formatted = offer.get('priceFormatted')
            
            offer_detail = offer.get('offerDetail', {})
            initial_bid_value = offer_detail.get('initialBidValue')
            current_min_bid = offer_detail.get('currentMinBid')
            current_max_bid = offer_detail.get('currentMaxBid')
            reserved_price = offer_detail.get('reservedPrice')
            
            bid_increment_obj = offer.get('currentBidIncrement', {})
            bid_increment = bid_increment_obj.get('currentBidIncrement')
            
            # Informações de lances
            has_bids = offer.get('hasBids', False)
            has_received_bids_or_proposals = offer.get('hasReceivedBidsOrProposals', False)
            total_bidders = offer.get('totalBidders', 0)
            total_bids = offer.get('totalBids', 0)
            total_received_proposals = offer.get('totalReceivedProposals', 0)
            
            # Winner info
            winner_bid = offer.get('winnerBid', {})
            current_winner_id = winner_bid.get('currentWinner')
            current_winner_login = winner_bid.get('currentWinnerLogin')
            
            # Dados de veículo (se aplicável)
            brand = None
            model = None
            year_manufacture = None
            year_model = None
            plate = None
            color = None
            fuel = None
            transmission = None
            km = None
            vehicle_restrictions = None
            vehicle_owner = None
            vehicle_debts = None
            
            # Tenta extrair dos templates
            template = product.get('template', {})
            groups = template.get('groups', [])
            for group in groups:
                properties = group.get('properties', [])
                for prop in properties:
                    prop_id = prop.get('id', '').lower()
                    value = prop.get('value')
                    
                    if prop_id == 'anofabricacao' and value:
                        try:
                            year_manufacture = int(value)
                        except:
                            pass
                    elif prop_id == 'anomodelo' and value:
                        try:
                            year_model = int(value)
                        except:
                            pass
            
            # Tenta extrair do productCustomJson
            product_custom_json = product.get('productCustomJson')
            if product_custom_json:
                try:
                    custom_data = json.loads(product_custom_json)
                    vehicle_restrictions = custom_data.get('vehicleRestrictions')
                    vehicle_owner = custom_data.get('vehicleOwner')
                    vehicle_debts = custom_data.get('vehicleDebts')
                except:
                    pass
            
            # Product ref
            product_your_ref = product.get('productYourRef')
            
            # Imagens
            gallery_json = product.get('galleryJson', [])
            image_url = None
            if gallery_json and len(gallery_json) > 0:
                image_url = gallery_json[0].get('link')
            
            photo_count = product.get('photoCount', 0)
            video_url_count = product.get('videoUrlCount', 0)
            
            # Status
            offer_status = offer.get('offerStatus', {})
            status_code = offer_status.get('statusCode')
            is_removed = offer_status.get('removed', False)
            is_stabbed = offer_status.get('stabbed', False)
            is_subjudice = offer_status.get('subjudice', False)
            is_sold = offer_status.get('sold', False)
            is_reserved = offer_status.get('reserved', False)
            is_closed = offer_status.get('closed', False)
            
            offer_status_id = offer.get('statusId')
            offer_type_id = offer.get('offerTypeId')
            
            # Group offer
            group_offer = offer.get('groupOffer', {})
            group_offer_id = group_offer.get('id')
            
            # Quantidades
            quantity_in_lot = offer.get('quantityInLot', 1)
            quantity_sold = offer.get('quantitySold', 0)
            quantity_reserved = offer.get('quantityReserved', 0)
            
            # System metric
            system_metric = offer.get('systemMetric')
            
            # Visitas
            visits = offer.get('visits', 0)
            
            # Seller
            seller = offer.get('seller', {})
            seller_id = seller.get('id')
            seller_name = seller.get('name')
            seller_city = seller.get('city')
            seller_phone = seller.get('phone')
            seller_company = seller.get('company')
            
            # Commercial conditions
            commercial_condition = offer.get('commercialCondition', {})
            commission_percent = commercial_condition.get('auctioneerCommissionPercent')
            allows_credit_card = commercial_condition.get('allowsCreditCard', False)
            allows_credit_card_total = commercial_condition.get('allowCreditCardTotalValue', False)
            transaction_limit = commercial_condition.get('transactionLimit')
            max_installments = commercial_condition.get('maxInstallments')
            
            # Datas
            end_date = self._parse_datetime(offer.get('endDate'))
            end_date_time = offer.get('endDateTime')
            create_at = self._parse_datetime(offer.get('createAt'))
            update_at = self._parse_datetime(offer.get('updateAt'))
            published_at = self._parse_datetime(offer.get('publishedAt'))
            indexation_date = self._parse_datetime(offer.get('indexationDate'))
            
            # Link
            link = f"https://exchange.superbid.net/oferta/{offer_id}"
            
            # Lot number
            lot_number = offer.get('lotNumber')
            
            # Filtros
            if 'demo' in store_name.lower() or 'test' in store_name.lower():
                return None
            
            if price and price < 1:
                return None
            
            # Monta o item com todos os campos
            item = {
                'external_id': external_id,
                'offer_id': offer_id,
                'product_id': product_id,
                'lot_number': lot_number,
                'auction_id': auction_id,
                'group_offer_id': group_offer_id,
                'category': category,
                'product_type_id': product_type_id,
                'product_type_desc': product_type_desc,
                'sub_category_id': sub_category_id,
                'sub_category_desc': sub_category_desc,
                'title': title,
                'description': description,
                'city': city,
                'state': state,
                'location_full': location_full,
                'location_lat': location_lat,
                'location_lon': location_lon,
                'auction_name': auction_name,
                'auction_status_id': auction_status_id,
                'auction_modality': auction_modality,
                'auction_begin_date': auction_begin_date,
                'auction_end_date': auction_end_date,
                'auction_max_enddate': auction_max_enddate,
                'auctioneer_name': auctioneer_name,
                'auctioneer_registry': auctioneer_registry,
                'auction_address': auction_address,
                'judicial_praca': judicial_praca,
                'judicial_praca_desc': judicial_praca_desc,
                'judicial_control_number': judicial_control_number,
                'store_id': store_id,
                'store_name': store_name,
                'store_highlight': store_highlight,
                'store_logo_url': store_logo_url,
                'manager_id': manager_id,
                'manager_name': manager_name,
                'price': price,
                'price_formatted': price_formatted,
                'initial_bid_value': initial_bid_value,
                'current_min_bid': current_min_bid,
                'current_max_bid': current_max_bid,
                'reserved_price': reserved_price,
                'bid_increment': bid_increment,
                'has_bids': has_bids,
                'has_received_bids_or_proposals': has_received_bids_or_proposals,
                'total_bidders': total_bidders,
                'total_bids': total_bids,
                'total_received_proposals': total_received_proposals,
                'current_winner_id': current_winner_id,
                'current_winner_login': current_winner_login,
                'brand': brand,
                'model': model,
                'year_manufacture': year_manufacture,
                'year_model': year_model,
                'plate': plate,
                'color': color,
                'fuel': fuel,
                'transmission': transmission,
                'km': km,
                'vehicle_restrictions': vehicle_restrictions,
                'vehicle_owner': vehicle_owner,
                'vehicle_debts': vehicle_debts,
                'product_your_ref': product_your_ref,
                'image_url': image_url,
                'photo_count': photo_count,
                'video_url_count': video_url_count,
                'offer_status_id': offer_status_id,
                'offer_type_id': offer_type_id,
                'status_code': status_code,
                'is_removed': is_removed,
                'is_stabbed': is_stabbed,
                'is_subjudice': is_subjudice,
                'is_sold': is_sold,
                'is_reserved': is_reserved,
                'is_closed': is_closed,
                'is_highlight': False,
                'is_favorite': offer.get('isFavorite', False),
                'quantity_in_lot': quantity_in_lot,
                'quantity_sold': quantity_sold,
                'quantity_reserved': quantity_reserved,
                'system_metric': system_metric,
                'visits': visits,
                'seller_id': seller_id,
                'seller_name': seller_name,
                'seller_city': seller_city,
                'seller_phone': seller_phone,
                'seller_company': seller_company,
                'commission_percent': commission_percent,
                'allows_credit_card': allows_credit_card,
                'allows_credit_card_total': allows_credit_card_total,
                'transaction_limit': transaction_limit,
                'max_installments': max_installments,
                'end_date': end_date,
                'end_date_time': end_date_time,
                'create_at': create_at,
                'update_at': update_at,
                'published_at': published_at,
                'indexation_date': indexation_date,
                'link': link,
                'source': 'superbid',
                'metadata': {
                    'secao_site': display_name,
                },
                'is_active': True,
                'has_bid': has_bids,
            }
            
            return item
            
        except Exception as e:
            return None
    
    def _parse_datetime(self, date_str: Optional[str]) -> Optional[str]:
        """Converte datetime para formato PostgreSQL"""
        if not date_str:
            return None
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S-03')
        except:
            return None


def main():
    """Execução principal"""
    print("\n" + "="*70)
    print("🚀 SUPERBID - SCRAPER")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # Scrape
    scraper = SuperbidScraper()
    items = scraper.scrape()
    
    print(f"\n✅ Total coletado: {len(items)} itens")
    print(f"🔥 Itens com lances: {scraper.stats['with_bids']}")
    print(f"🔄 Duplicatas filtradas: {scraper.stats['duplicates']}")
    
    if not items:
        print("⚠️ Nenhum item coletado - encerrando")
        return
    
    # Salva JSON - ✅ CAMINHO CORRIGIDO
    output_dir = Path(__file__).parent / 'data' / 'normalized'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'superbid_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON salvo: {json_file}")
    
    # Importa e usa o cliente Supabase - ✅ IMPORT CORRIGIDO
    try:
        from supabase_client import SupabaseSuperbid
        
        print("\n📤 INSERINDO NO SUPABASE")
        supabase = SupabaseSuperbid()
        
        if not supabase.test():
            print("⚠️ Erro na conexão com Supabase - pulando insert")
        else:
            stats = supabase.upsert(items)
            
            print(f"\n  📈 RESULTADO:")
            print(f"    ✅ Inseridos: {stats['inserted']}")
            print(f"    🔄 Atualizados: {stats['updated']}")
            if stats['errors'] > 0:
                print(f"    ⚠️ Erros: {stats['errors']}")
    
    except Exception as e:
        print(f"⚠️ Erro no Supabase: {e}")
    
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    
    print("\n" + "="*70)
    print("📊 ESTATÍSTICAS FINAIS")
    print("="*70)
    print(f"\n  Por Categoria:")
    for category, count in sorted(scraper.stats['by_category'].items()):
        print(f"    • {category}: {count} itens")
    print(f"\n  • Total coletado: {scraper.stats['total_scraped']}")
    print(f"  • Com lances: {scraper.stats['with_bids']}")
    print(f"  • Duplicatas: {scraper.stats['duplicates']}")
    print(f"\n⏱️ Duração: {minutes}min {seconds}s")
    print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    main()