#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPERBID SCRAPER - OTIMIZADO PARA ML + HEARTBEAT
✅ Passive listening completo
✅ 18 categorias principais
✅ Upload automático para Supabase
✅ Sistema de heartbeat integrado (infra_actions)
"""

import sys
import json
import time
import os
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# Importa o cliente Supabase (da pasta pai)
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from supabase_client import SupabaseSuperbid


class SuperbidScraper:
    """Scraper Superbid otimizado para análise ML"""
    
    def __init__(self):
        self.source = 'superbid'
        self.base_url = 'https://offer-query.superbid.net/seo/offers/'
        self.site_url = 'https://exchange.superbid.net'
        
        # 18 CATEGORIAS PRINCIPAIS
        self.categories = [
            ('alimentos-e-bebidas', 'Alimentos e Bebidas'),
            ('animais', 'Animais'),
            ('bolsas-canetas-joias-e-relogios', 'Bolsas, Canetas, Joias e Relógios'),
            ('caminhoes-onibus', 'Caminhões e Ônibus'),
            ('carros-motos', 'Carros e Motos'),
            ('cozinhas-e-restaurantes', 'Cozinhas e Restaurantes'),
            ('eletrodomesticos', 'Eletrodomésticos'),
            ('materiais-para-construcao-civil', 'Materiais para Construção Civil'),
            ('maquinas-pesadas-agricolas', 'Máquinas Pesadas e Agrícolas'),
            ('industrial-maquinas-equipamentos', 'Industrial, Máquinas e Equipamentos'),
            ('imoveis', 'Imóveis'),
            ('embarcacoes-aeronaves', 'Embarcações e Aeronaves'),
            ('moveis-e-decoracao', 'Móveis e Decoração'),
            ('movimentacao-transporte', 'Movimentação e Transporte'),
            ('oportunidades', 'Oportunidades'),
            ('partes-e-pecas', 'Partes e Peças'),
            ('sucatas-materiais-residuos', 'Sucatas, Materiais e Resíduos'),
            ('tecnologia', 'Tecnologia'),
        ]
        
        # Mapeamento direto: categorias Superbid → 10 categorias refinadas
        self.category_mapping = {
            'Imóveis': 'Imóveis',
            'Carros e Motos': 'Veículos',
            'Caminhões e Ônibus': 'Veículos',
            'Embarcações e Aeronaves': 'Veículos',
            'Máquinas Pesadas e Agrícolas': 'Máquinas & Equipamentos',
            'Industrial, Máquinas e Equipamentos': 'Máquinas & Equipamentos',
            'Movimentação e Transporte': 'Máquinas & Equipamentos',
            'Tecnologia': 'Tecnologia',
            'Eletrodomésticos': 'Casa & Consumo',
            'Móveis e Decoração': 'Casa & Consumo',
            'Alimentos e Bebidas': 'Casa & Consumo',
            'Cozinhas e Restaurantes': 'Industrial & Empresarial',
            'Partes e Peças': 'Industrial & Empresarial',
            'Sucatas, Materiais e Resíduos': 'Materiais & Sucatas',
            'Materiais para Construção Civil': 'Materiais & Sucatas',
            'Animais': 'Animais',
            'Bolsas, Canetas, Joias e Relógios': 'Arte & Colecionáveis',
            'Oportunidades': 'Outros',
        }
        
        self.stats = {
            'total_scraped': 0,
            'by_category': {},
            'duplicates': 0,
            'with_bids': 0,
            'errors': 0,
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
    
    def _categorize_item(self, original_category: str) -> str:
        """
        Mapeia categoria original do Superbid para uma das 10 categorias refinadas
        """
        return self.category_mapping.get(original_category, 'Outros')
    
    def scrape(self) -> List[Dict]:
        """Scrape completo de todas as categorias"""
        print("\n" + "="*80)
        print("🔵 SUPERBID - SCRAPER OTIMIZADO PARA ML")
        print("="*80)
        print(f"📦 Categorias: {len(self.categories)}")
        print("🎯 Foco: Campos essenciais para análise e oportunidades")
        print("="*80 + "\n")
        
        all_items = []
        global_ids = set()
        
        for idx, (url_slug, display_name) in enumerate(self.categories, 1):
            print(f"\n[{idx}/{len(self.categories)}] 📦 {display_name}")
            print(f"{'─'*80}")
            
            category_items = self._scrape_category(
                url_slug, display_name, global_ids
            )
            
            all_items.extend(category_items)
            self.stats['by_category'][display_name] = len(category_items)
            
            print(f"   ✅ {len(category_items)} itens coletados")
            
            time.sleep(2)
        
        self.stats['total_scraped'] = len(all_items)
        return all_items
    
    def _scrape_category(self, url_slug: str, display_name: str, 
                        global_ids: set) -> List[Dict]:
        """Scrape completo de uma categoria (todas as páginas)"""
        items = []
        page_num = 1
        page_size = 100
        consecutive_errors = 0
        max_errors = 3
        
        while True:
            try:
                params = {
                    "urlSeo": f"{self.site_url}/categorias/{url_slug}",
                    "locale": "pt_BR",
                    "orderBy": "score:desc",
                    "pageNumber": page_num,
                    "pageSize": page_size,
                    "portalId": "[2,15]",
                    "requestOrigin": "marketplace",
                    "searchType": "opened" if url_slug == 'imoveis' else "openedAll",
                    "timeZoneId": "America/Sao_Paulo",
                }
                
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=30
                )
                
                if response.status_code != 200:
                    consecutive_errors += 1
                    print(f"   ⚠️  Erro HTTP {response.status_code} na página {page_num}")
                    if consecutive_errors >= max_errors:
                        break
                    page_num += 1
                    time.sleep(3)
                    continue
                
                data = response.json()
                offers = data.get('offers', [])
                total_offers = data.get('total', 0)
                
                if not offers:
                    break
                
                consecutive_errors = 0
                print(f"   📄 Página {page_num}: {len(offers)} ofertas (total: {total_offers})")
                
                page_items = 0
                for offer_data in offers:
                    item = self._parse_offer(offer_data, display_name)
                    
                    if item and item['external_id'] not in global_ids:
                        items.append(item)
                        global_ids.add(item['external_id'])
                        page_items += 1
                        
                        if item.get('has_bids'):
                            self.stats['with_bids'] += 1
                    elif item:
                        self.stats['duplicates'] += 1
                
                # Verifica se há mais páginas
                start = data.get('start', 0)
                limit = data.get('limit', page_size)
                if start + limit >= total_offers:
                    break
                
                page_num += 1
                time.sleep(1)
                
            except Exception as e:
                consecutive_errors += 1
                self.stats['errors'] += 1
                print(f"   ⚠️  Erro: {str(e)[:100]}")
                if consecutive_errors >= max_errors:
                    break
                page_num += 1
                time.sleep(3)
        
        return items
    
    def _parse_offer(self, offer: Dict, category_display: str) -> Optional[Dict]:
        """Parse - preserva raw_data completo"""
        try:
            offer_id = offer.get('id')
            if not offer_id:
                return None
            
            return {
                'external_id': f"superbid_{offer_id}",
                'category': category_display,  # categoria original do Superbid
                'refined_category': self._categorize_item(category_display),  # categoria refinada (10 categorias)
                'scraped_at': datetime.now().isoformat(),
                'raw_data': offer,  # TODOS os dados da API
                'offer_id': offer_id,
                'has_bids': offer.get('hasBids', False),
                'link': f"https://exchange.superbid.net/oferta/{offer_id}",
            }
            
        except Exception as e:
            self.stats['errors'] += 1
            return None
    
    def save(self, items: List[Dict], output_dir: Path = None) -> Path:
        """Salva dados coletados (backup local)"""
        if output_dir is None:
            output_dir = Path(__file__).parent / 'data'
        
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file = output_dir / f'superbid_{timestamp}.json'
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        
        return json_file
    
    def print_stats(self):
        """Imprime estatísticas finais"""
        print("\n" + "="*80)
        print("📊 ESTATÍSTICAS FINAIS")
        print("="*80)
        
        print(f"\n📦 Por Categoria:")
        for category, count in sorted(self.stats['by_category'].items()):
            print(f"   • {category:<45} {count:>5} itens")
        
        print(f"\n📈 Resumo:")
        print(f"   • Total: {self.stats['total_scraped']}")
        print(f"   • Com lances: {self.stats['with_bids']}")
        print(f"   • Duplicatas: {self.stats['duplicates']}")
        print(f"   • Erros: {self.stats['errors']}")
        
        print("\n" + "="*80)


def main():
    """Execução principal - Scrape + Upload"""
    print("\n" + "="*80)
    print("🚀 SUPERBID - SCRAPER + UPLOAD")
    print("="*80)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    start_time = time.time()
    
    # ========================================
    # INICIALIZAÇÃO HEARTBEAT
    # ========================================
    supabase = None
    try:
        if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_SERVICE_ROLE_KEY'):
            print("\n⚠️ Variáveis SUPABASE não configuradas - heartbeat desabilitado")
        else:
            supabase = SupabaseSuperbid(service_name='superbid_scraper')
            
            # ✅ HEARTBEAT: Registra início
            supabase.heartbeat_start({'categories': 18})
    except Exception as e:
        print(f"\n⚠️ Erro ao inicializar heartbeat: {e}")
    
    # ========================================
    # ETAPA 1: SCRAPING
    # ========================================
    scraper = SuperbidScraper()
    
    try:
        items = scraper.scrape()
    except Exception as e:
        # ✅ HEARTBEAT: Registra erro fatal
        if supabase:
            supabase.heartbeat_error(e, context="scrape_main")
        raise
    
    if not items:
        print("\n⚠️  Nenhum item coletado")
        return 1
    
    # Salva backup local
    json_file = scraper.save(items)
    print(f"\n💾 Backup local: {json_file}")
    
    scraper.print_stats()
    
    # ========================================
    # ETAPA 2: UPLOAD SUPABASE
    # ========================================
    print("\n" + "="*80)
    print("🔵 UPLOAD PARA SUPABASE")
    print("="*80)
    
    try:
        if not supabase:
            if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_SERVICE_ROLE_KEY'):
                print("\n⚠️ Variáveis SUPABASE não configuradas - pulando upload")
                return 1
            else:
                supabase = SupabaseSuperbid(service_name='superbid_scraper')
        
        stats = supabase.upsert(items)
        
        print("\n" + "="*80)
        print("📊 RESULTADO DO UPLOAD")
        print("="*80)
        print(f"   ✅ Inseridos: {stats['inserted']}")
        print(f"   🔄 Atualizados: {stats['updated']}")
        print(f"   ❌ Erros: {stats['errors']}")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Erro no upload Supabase: {str(e)}")
        print("⚠️  Dados foram salvos localmente em:", json_file)
        
        # ✅ HEARTBEAT: Registra erro no insert
        if supabase:
            supabase.heartbeat_error(e, context="supabase_insert")
        
        return 1
    
    # ========================================
    # RESUMO FINAL
    # ========================================
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    
    print(f"\n⏱️  Duração: {minutes}min {seconds}s")
    print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # ✅ HEARTBEAT: Registra sucesso com estatísticas finais
    if supabase:
        supabase.heartbeat_success(final_stats={
            'total_items': len(items),
            'categories_scraped': len(scraper.categories),
            'with_bids': scraper.stats['with_bids'],
            'by_category': scraper.stats['by_category'],
            'duration_seconds': round(elapsed, 2)
        })
    
    print("="*80 + "\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())